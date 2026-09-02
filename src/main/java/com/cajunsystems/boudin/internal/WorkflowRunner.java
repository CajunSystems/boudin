package com.cajunsystems.boudin.internal;

import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.boudin.query.QueryMessage;
import com.cajunsystems.boudin.query.QuerySerializer;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.boudin.workflow.WorkflowContext;
import com.cajunsystems.boudin.workflow.WorkflowThread;
import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the lifecycle of a single workflow instance.
 *
 * <p>Created by {@link WorkflowDispatcher} for each {@link HistoryEvent.WorkflowStarted}
 * event (both new and crash-recovered workflows). Responsible for:
 * <ol>
 *   <li>Creating the {@link WorkflowContext} (with {@link ReplayState} if recovering).</li>
 *   <li>Subscribing to {@code workflow-history:{workflowId}} from the position after the
 *       last known history entry to receive live events (activity results, signals).</li>
 *   <li>Routing incoming history events to the {@link WorkflowContext} to unblock
 *       the workflow virtual thread.</li>
 *   <li>Subscribing to {@code workflow-queries:{workflowId}} and answering
 *       {@link QueryMessage.QueryRequested} messages from the workflow implementation.</li>
 *   <li>Starting the {@link WorkflowThread} which runs the workflow method.</li>
 * </ol>
 *
 * <p>For crash recovery, the existing history is passed at construction so that
 * {@link ReplayState} can pre-populate its result cache. Historical signals are
 * replayed onto the workflow implementation before the virtual thread starts, so
 * that {@code Workflow.await()} conditions see the correct field values immediately.
 */
public class WorkflowRunner {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRunner.class);

    private final String workflowId;
    private final String taskQueue;
    private final SharedLog sharedLog;
    private final WorkflowRegistry workflowRegistry;
    private final List<HistoryEvent> existingHistory;
    private final long lastHistorySeqnum;

    private final Runnable onComplete;
    private final BoudinEventLoop eventLoop;
    private final HashedWheelTimer timerWheel;
    private final BoudinMetrics metrics;

    private WorkflowContext context;
    private WorkflowThread workflowThread;
    private SharedLog.Subscription historySubscription;
    private SharedLog.Subscription querySubscription;
    private final Set<Long> processedEventSeqnums = ConcurrentHashMap.newKeySet();
    private final Set<String> answeredQueryIds = ConcurrentHashMap.newKeySet();

    public WorkflowRunner(String workflowId,
                          String taskQueue,
                          SharedLog sharedLog,
                          WorkflowRegistry workflowRegistry,
                          List<HistoryEvent> existingHistory,
                          long lastHistorySeqnum,
                          Runnable onComplete,
                          BoudinEventLoop eventLoop,
                          HashedWheelTimer timerWheel,
                          BoudinMetrics metrics) {
        this.workflowId = workflowId;
        this.taskQueue = taskQueue;
        this.sharedLog = sharedLog;
        this.workflowRegistry = workflowRegistry;
        this.existingHistory = existingHistory;
        this.lastHistorySeqnum = lastHistorySeqnum;
        this.onComplete = onComplete;
        this.eventLoop = eventLoop;
        this.timerWheel = timerWheel;
        this.metrics = metrics;
    }

    /**
     * Starts execution of the workflow instance.
     *
     * <ol>
     *   <li>Creates the workflow impl instance and resolves the workflow method.</li>
     *   <li>Builds {@link ReplayState} from existing history.</li>
     *   <li>Creates {@link WorkflowContext} and subscribes to history events.</li>
     *   <li>Replays historical signals onto the impl (before the thread starts) so
     *       that {@code Workflow.await()} conditions see correct field values.</li>
     *   <li>Starts the workflow virtual thread.</li>
     * </ol>
     */
    public void start(HistoryEvent.WorkflowStarted startedEvent) {
        String workflowType = startedEvent.workflowType();

        // Instantiate workflow impl
        Object impl = workflowRegistry.createInstance(workflowType);
        Method workflowMethod = workflowRegistry.findWorkflowMethod(workflowType);

        // Build replay state from existing history
        ReplayState replayState = new ReplayState(existingHistory, lastHistorySeqnum);

        // Create context
        context = new WorkflowContext(
                workflowId, workflowType, taskQueue, sharedLog, replayState, timerWheel);

        // Subscribe to workflow-history:{workflowId} for LIVE events only
        // (from after the last known history entry so we don't re-deliver replayed events)
        long subscribeFrom = lastHistorySeqnum + 1;
        LogTag historyTag = LogTag.of("workflow-history", workflowId);
        LogView historyView = sharedLog.getView(historyTag);

        historySubscription = historyView.subscribe(
                new LogPosition(subscribeFrom),
                entry -> {
                    long seqnum = entry.seqnum();
                    HistoryEvent event = HistorySerializer.INSTANCE.deserialize(entry.data());
                    eventLoop.submit(() -> onHistoryEvent(seqnum, event, impl, workflowType));
                }
        );

        // Subscribe to workflow-queries:{workflowId} for control-plane query requests.
        // Read from BEGINNING: a client may have appended its request before this runner
        // existed, and query messages are never part of the replayed history.
        LogTag queryTag = LogTag.of("workflow-queries", workflowId);
        querySubscription = sharedLog.getView(queryTag).subscribe(
                LogPosition.BEGINNING,
                entry -> {
                    QueryMessage message = QuerySerializer.INSTANCE.deserialize(entry.data());
                    if (message instanceof QueryMessage.QueryRequested request) {
                        eventLoop.submit(() -> onQueryRequested(request, impl, workflowType));
                    }
                }
        );

        // Replay historical signals BEFORE starting the workflow thread.
        // This ensures signal-modified fields are visible when the thread evaluates
        // Workflow.await() conditions during replay.
        replayHistoricalSignals(impl, workflowType);

        // Start the workflow virtual thread
        workflowThread = new WorkflowThread(context, impl, workflowMethod, startedEvent.input(), metrics);
        workflowThread.start();

        log.info("Started workflow {} (type={}, replay={})",
                workflowId, workflowType, !existingHistory.isEmpty());
    }

    /** Delivers a live history event to the running workflow context. */
    private void onHistoryEvent(long seqnum, HistoryEvent event, Object impl, String workflowType) {
        if (!processedEventSeqnums.add(seqnum)) return;
        switch (event) {
            case HistoryEvent.ActivityCompleted ac ->
                    context.deliverActivityResult(ac.activityId(), ac.result());

            case HistoryEvent.ActivityFailed af ->
                    context.deliverActivityFailure(af.activityId(), af.errorType(), af.message());

            case HistoryEvent.SignalReceived sr -> {
                Method signalMethod = workflowRegistry.findSignalMethod(workflowType, sr.signalName());
                if (signalMethod != null) {
                    context.deliverSignal(signalMethod, impl, sr.payload());
                } else {
                    log.warn("No signal handler found for '{}' in workflow type {}",
                            sr.signalName(), workflowType);
                }
            }

            case HistoryEvent.TimerFired tf ->
                    context.deliverTimerFired(tf.timerId());

            case HistoryEvent.ChildWorkflowCompleted cwc ->
                    context.deliverChildWorkflowResult(cwc.childWorkflowId(), cwc.result());

            case HistoryEvent.ChildWorkflowFailed cwf ->
                    context.deliverChildWorkflowFailure(cwf.childWorkflowId(), cwf.errorType(), cwf.message());

            case HistoryEvent.WorkflowCompleted wc -> {
                log.debug("WorkflowCompleted observed in history for {}", workflowId);
                close();
                if (onComplete != null) onComplete.run();
            }

            case HistoryEvent.WorkflowFailed wf -> {
                log.debug("WorkflowFailed observed in history for {}", workflowId);
                close();
                if (onComplete != null) onComplete.run();
            }

            // WorkflowStarted, ActivityScheduled — informational; no action needed
            default -> {}
        }
    }

    /**
     * Answers a query request against this workflow instance.
     *
     * <p>Runs on the event loop only long enough to dedupe; the handler invocation and the
     * response append happen on a virtual thread, because the append blocks on {@code join()}
     * and must not stall dispatch for every other workflow on this worker.
     */
    private void onQueryRequested(QueryMessage.QueryRequested request, Object impl, String workflowType) {
        if (!answeredQueryIds.add(request.queryId())) return;

        Thread.ofVirtual().name("boudin-query-" + request.queryId()).start(() -> {
            QueryMessage response;
            Method queryMethod = workflowRegistry.findQueryMethod(workflowType, request.queryName());

            if (queryMethod == null) {
                log.warn("No query handler found for '{}' in workflow type {}",
                        request.queryName(), workflowType);
                response = new QueryMessage.QueryFailed(
                        request.queryId(), Instant.now(), workflowId, "UnknownQuery",
                        "No @QueryMethod named '" + request.queryName() + "' on " + workflowType);
            } else {
                try {
                    Object result = context.invokeQuery(queryMethod, impl, request.args());
                    byte[] resultBytes = result != null ? KryoSerializer.toBytes(result) : new byte[0];
                    response = new QueryMessage.QueryCompleted(
                            request.queryId(), Instant.now(), workflowId, resultBytes);
                } catch (Throwable t) {
                    log.warn("Query '{}' on workflow {} threw", request.queryName(), workflowId, t);
                    response = new QueryMessage.QueryFailed(
                            request.queryId(), Instant.now(), workflowId,
                            t.getClass().getSimpleName(), String.valueOf(t.getMessage()));
                }
            }

            try {
                sharedLog.append(AppendRequest.to(
                        LogTag.of("workflow-queries", workflowId),
                        QuerySerializer.INSTANCE.serialize(response))).join();
            } catch (Exception e) {
                log.error("Failed to append query response for query {} on workflow {}",
                        request.queryId(), workflowId, e);
            }
        });
    }

    /**
     * Replays all {@link HistoryEvent.SignalReceived} events from the existing history
     * directly onto the workflow implementation before the virtual thread starts.
     */
    private void replayHistoricalSignals(Object impl, String workflowType) {
        for (HistoryEvent event : existingHistory) {
            if (!(event instanceof HistoryEvent.SignalReceived sr)) continue;
            Method signalMethod = workflowRegistry.findSignalMethod(workflowType, sr.signalName());
            if (signalMethod == null) {
                log.warn("No signal handler found for '{}' during replay in workflow {}",
                        sr.signalName(), workflowId);
                continue;
            }
            try {
                context.deliverSignal(signalMethod, impl, sr.payload());
            } catch (Exception e) {
                log.error("Failed to replay signal '{}' for workflow {}",
                        sr.signalName(), workflowId, e);
            }
        }
    }

    /** Closes the history subscription, cancels pending timers, and interrupts the workflow thread. */
    public void close() {
        // Cancel any pending timers before closing the history subscription
        if (context != null) {
            for (String timerId : context.pendingTimerIds()) {
                timerWheel.cancel(timerId);
            }
        }
        if (historySubscription != null) {
            try { historySubscription.close(); } catch (Exception ignored) {}
            historySubscription = null;
        }
        if (querySubscription != null) {
            try { querySubscription.close(); } catch (Exception ignored) {}
            querySubscription = null;
        }
        if (workflowThread != null && workflowThread.thread().isAlive()) {
            workflowThread.thread().interrupt();
        }
    }

    public String workflowId() { return workflowId; }

    public WorkflowContext context() { return context; }
}
