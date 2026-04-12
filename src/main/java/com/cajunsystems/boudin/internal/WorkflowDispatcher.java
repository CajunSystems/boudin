package com.cajunsystems.boudin.internal;

import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Subscribes to the {@code workflow-tasks:{taskQueue}} log tag and creates
 * {@link WorkflowRunner} instances for each {@link HistoryEvent.WorkflowStarted} event.
 *
 * <h2>Startup and crash recovery</h2>
 * On {@link #start()}, this dispatcher reads <em>all</em> historical workflow task entries
 * from the beginning of the log. For each unfinished workflow (no {@code WorkflowCompleted}
 * or {@code WorkflowFailed} in its history), it loads the full history and starts a
 * {@link WorkflowRunner} in replay mode. Finished workflows are silently skipped.
 *
 * <p>After processing history, it subscribes from the current tail for live
 * {@code WorkflowStarted} events (new workflow executions).
 *
 * <h2>Idempotency</h2>
 * {@code knownWorkflowIds} tracks all workflow IDs that have been seen in this JVM session.
 * Even if the underlying subscription delivers the same event twice, the second delivery
 * is a no-op.
 */
public class WorkflowDispatcher {

    private static final Logger log = LoggerFactory.getLogger(WorkflowDispatcher.class);

    private final String taskQueue;
    private final SharedLog sharedLog;
    private final WorkflowRegistry workflowRegistry;

    private final Set<String> knownWorkflowIds = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, WorkflowRunner> runners = new ConcurrentHashMap<>();

    private SharedLog.Subscription liveSubscription;

    public WorkflowDispatcher(String taskQueue, SharedLog sharedLog,
                               WorkflowRegistry workflowRegistry) {
        this.taskQueue = taskQueue;
        this.sharedLog = sharedLog;
        this.workflowRegistry = workflowRegistry;
    }

    /**
     * Starts the dispatcher:
     * <ol>
     *   <li>Reads all historical workflow task events and resumes in-flight workflows.</li>
     *   <li>Subscribes to the tail of the task queue for new workflow starts.</li>
     * </ol>
     */
    public void start() {
        LogTag taskTag = LogTag.of("workflow-tasks", taskQueue);
        LogView taskView = sharedLog.getView(taskTag);

        // Phase 1: process historical events (crash recovery)
        log.info("WorkflowDispatcher[{}]: scanning historical workflow tasks...", taskQueue);
        List<HistoryEvent> historical = taskView.readAll().join().stream()
                .map(entry -> HistorySerializer.INSTANCE.deserialize(entry.data()))
                .toList();
        for (HistoryEvent event : historical) {
            if (event instanceof HistoryEvent.WorkflowStarted ws) {
                handleWorkflowStarted(ws);
            }
        }
        log.info("WorkflowDispatcher[{}]: finished historical scan ({} events), {} active workflows",
                taskQueue, historical.size(), runners.size());

        // Phase 2: subscribe to the tail for new workflow starts (live mode)
        liveSubscription = taskView.subscribeTail(entry -> {
            HistoryEvent event = HistorySerializer.INSTANCE.deserialize(entry.data());
            if (event instanceof HistoryEvent.WorkflowStarted ws) {
                handleWorkflowStarted(ws);
            }
        });
    }

    /**
     * Pre-registers a runner that was created by an external caller (e.g., a task poller).
     * Prevents the dispatcher from creating a duplicate runner for the same workflow ID.
     */
    public void registerRunner(String workflowId, WorkflowRunner runner) {
        knownWorkflowIds.add(workflowId);
        runners.put(workflowId, runner);
    }

    /** Stops all subscriptions and closes all runners. */
    public void stop() {
        if (liveSubscription != null) {
            try { liveSubscription.close(); } catch (Exception ignored) {}
        }
        runners.values().forEach(WorkflowRunner::close);
        runners.clear();
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private void handleWorkflowStarted(HistoryEvent.WorkflowStarted ws) {
        // Idempotency: skip if already handling or already completed
        if (!knownWorkflowIds.add(ws.workflowId())) return;

        // Skip workflows for types we don't have registered
        if (!workflowRegistry.isRegistered(ws.workflowType())) {
            log.debug("WorkflowDispatcher[{}]: ignoring workflow {} (type {} not registered)",
                    taskQueue, ws.workflowId(), ws.workflowType());
            return;
        }

        // Load the full history to check completion status and enable replay
        LogTag historyTag = LogTag.of("workflow-history", ws.workflowId());
        LogView historyView = sharedLog.getView(historyTag);

        List<HistoryEvent> history = historyView.readAll().join().stream()
                .map(entry -> HistorySerializer.INSTANCE.deserialize(entry.data()))
                .toList();
        long lastSeqnum = computeLastSeqnum(historyTag, history);

        boolean isComplete = history.stream().anyMatch(
                e -> e instanceof HistoryEvent.WorkflowCompleted
                  || e instanceof HistoryEvent.WorkflowFailed);

        if (isComplete) {
            log.debug("WorkflowDispatcher[{}]: workflow {} is already complete, skipping",
                    taskQueue, ws.workflowId());
            return;
        }

        WorkflowRunner runner = new WorkflowRunner(
                ws.workflowId(), taskQueue, sharedLog, workflowRegistry,
                history, lastSeqnum);
        runners.put(ws.workflowId(), runner);
        runner.start(ws);
    }

    private long computeLastSeqnum(LogTag historyTag, List<HistoryEvent> history) {
        if (history.isEmpty()) return 0L;
        // Read raw entries to get seqnums — we need the last entry's global seqnum
        try {
            List<com.cajunsystems.gumbo.core.LogEntry> rawEntries =
                    sharedLog.getView(historyTag).readAll().join();
            if (!rawEntries.isEmpty()) {
                return rawEntries.getLast().seqnum();
            }
        } catch (Exception e) {
            log.warn("Could not determine last seqnum for workflow-history:{}", historyTag, e);
        }
        return 0L;
    }
}
