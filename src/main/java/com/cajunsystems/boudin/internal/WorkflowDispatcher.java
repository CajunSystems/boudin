package com.cajunsystems.boudin.internal;

import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Subscribes to the {@code workflow-tasks:{taskQueue}} log tag and creates
 * {@link WorkflowRunner} instances for each {@link HistoryEvent.WorkflowStarted} event.
 *
 * <h2>Startup and crash recovery</h2>
 * On {@link #start()}, this dispatcher reads the KV-persisted active-workflow set from
 * the {@code workflow-tasks:{queue}} tag. If the set is non-empty, only those in-flight
 * workflows are recovered (fast path). If the set is empty (first startup or data loss),
 * the full task log is scanned and the KV set is seeded from it.
 *
 * <p>After startup, it subscribes from the current tail for live {@code WorkflowStarted}
 * events (new workflow executions).
 *
 * <h2>Active-workflow KV set</h2>
 * Key: {@code "active-workflows"} on the {@code workflow-tasks:{queue}} tag.
 * Value: Kryo-serialized {@code HashSet<String>} of workflow IDs.
 * Updated on every workflow start and completion.
 */
public class WorkflowDispatcher {

    private static final Logger log = LoggerFactory.getLogger(WorkflowDispatcher.class);
    private static final String KV_ACTIVE_WORKFLOWS = "active-workflows";

    private final String taskQueue;
    private final SharedLog sharedLog;
    private final WorkflowRegistry workflowRegistry;
    private final BoudinEventLoop eventLoop;
    private final HashedWheelTimer timerWheel;

    private LogView taskView;
    private final Set<String> knownWorkflowIds = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, WorkflowRunner> runners = new ConcurrentHashMap<>();
    private final Set<String> activeWorkflowIds = new HashSet<>();
    private final Object activeWorkflowsLock = new Object();

    private SharedLog.Subscription liveSubscription;

    public WorkflowDispatcher(String taskQueue, SharedLog sharedLog,
                               WorkflowRegistry workflowRegistry, BoudinEventLoop eventLoop,
                               HashedWheelTimer timerWheel) {
        this.taskQueue = taskQueue;
        this.sharedLog = sharedLog;
        this.workflowRegistry = workflowRegistry;
        this.eventLoop = eventLoop;
        this.timerWheel = timerWheel;
    }

    public void start() {
        LogTag taskTag = LogTag.of("workflow-tasks", taskQueue);
        taskView = sharedLog.getView(taskTag);

        Set<String> savedActive = loadActiveWorkflows();

        if (savedActive.isEmpty()) {
            log.info("WorkflowDispatcher[{}]: no KV checkpoint, scanning full workflow-tasks log...", taskQueue);
            List<HistoryEvent> historical = taskView.readAll().join().stream()
                    .map(entry -> HistorySerializer.INSTANCE.deserialize(entry.data()))
                    .toList();
            for (HistoryEvent event : historical) {
                if (event instanceof HistoryEvent.WorkflowStarted ws) {
                    handleWorkflowStarted(ws);
                }
            }
            log.info("WorkflowDispatcher[{}]: full scan complete ({} events, {} active)",
                    taskQueue, historical.size(), runners.size());
        } else {
            log.info("WorkflowDispatcher[{}]: recovering {} in-flight workflows from KV checkpoint",
                    taskQueue, savedActive.size());
            synchronized (activeWorkflowsLock) {
                activeWorkflowIds.addAll(savedActive);
            }
            for (String workflowId : savedActive) {
                recoverWorkflow(workflowId);
            }
            log.info("WorkflowDispatcher[{}]: KV recovery complete", taskQueue);
        }

        liveSubscription = taskView.subscribeTail(entry -> {
            HistoryEvent event = HistorySerializer.INSTANCE.deserialize(entry.data());
            if (event instanceof HistoryEvent.WorkflowStarted ws) {
                handleWorkflowStarted(ws);
            }
        });
    }

    /**
     * Pre-registers a runner created by an external caller (e.g., a task poller).
     * Prevents the dispatcher from creating a duplicate runner for the same workflow ID.
     * Note: runner registered externally has no completion callback for active-set cleanup.
     */
    public void registerRunner(String workflowId, WorkflowRunner runner) {
        knownWorkflowIds.add(workflowId);
        runners.put(workflowId, runner);
    }

    public void stop() {
        if (liveSubscription != null) {
            try { liveSubscription.close(); } catch (Exception ignored) {}
        }
        runners.values().forEach(WorkflowRunner::close);
        runners.clear();
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private void recoverWorkflow(String workflowId) {
        if (!knownWorkflowIds.add(workflowId)) return;

        LogTag historyTag = LogTag.of("workflow-history", workflowId);
        LogView historyView = sharedLog.getView(historyTag);

        List<HistoryEvent> history = historyView.readAll().join().stream()
                .map(entry -> HistorySerializer.INSTANCE.deserialize(entry.data()))
                .toList();
        long lastSeqnum = computeLastSeqnum(historyTag, history);

        boolean isComplete = history.stream().anyMatch(
                e -> e instanceof HistoryEvent.WorkflowCompleted
                  || e instanceof HistoryEvent.WorkflowFailed);

        if (isComplete) {
            log.debug("WorkflowDispatcher[{}]: workflow {} is complete, removing from active set",
                    taskQueue, workflowId);
            removeFromActiveSet(workflowId);
            return;
        }

        HistoryEvent.WorkflowStarted startedEvent = (HistoryEvent.WorkflowStarted) history.stream()
                .filter(e -> e instanceof HistoryEvent.WorkflowStarted)
                .findFirst()
                .orElse(null);
        if (startedEvent == null) {
            log.warn("WorkflowDispatcher[{}]: no WorkflowStarted found for {}, skipping recovery",
                    taskQueue, workflowId);
            removeFromActiveSet(workflowId);
            return;
        }

        WorkflowRunner runner = new WorkflowRunner(
                workflowId, taskQueue, sharedLog, workflowRegistry,
                history, lastSeqnum,
                () -> onWorkflowComplete(workflowId),
                eventLoop, timerWheel);
        runners.put(workflowId, runner);
        runner.start(startedEvent);
    }

    private void handleWorkflowStarted(HistoryEvent.WorkflowStarted ws) {
        if (!knownWorkflowIds.add(ws.workflowId())) return;

        if (!workflowRegistry.isRegistered(ws.workflowType())) {
            log.debug("WorkflowDispatcher[{}]: ignoring workflow {} (type {} not registered)",
                    taskQueue, ws.workflowId(), ws.workflowType());
            return;
        }

        addToActiveSet(ws.workflowId());

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
            removeFromActiveSet(ws.workflowId());
            return;
        }

        WorkflowRunner runner = new WorkflowRunner(
                ws.workflowId(), taskQueue, sharedLog, workflowRegistry,
                history, lastSeqnum,
                () -> onWorkflowComplete(ws.workflowId()),
                eventLoop, timerWheel);
        runners.put(ws.workflowId(), runner);
        runner.start(ws);
    }

    private void onWorkflowComplete(String workflowId) {
        runners.remove(workflowId);
        removeFromActiveSet(workflowId);
        log.debug("WorkflowDispatcher[{}]: workflow {} removed from active set", taskQueue, workflowId);
    }

    private void addToActiveSet(String workflowId) {
        synchronized (activeWorkflowsLock) {
            activeWorkflowIds.add(workflowId);
            persistActiveWorkflows();
        }
    }

    private void removeFromActiveSet(String workflowId) {
        synchronized (activeWorkflowsLock) {
            activeWorkflowIds.remove(workflowId);
            persistActiveWorkflows();
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> loadActiveWorkflows() {
        if (taskView == null) return new HashSet<>();
        try {
            byte[] data = taskView.getValue(KV_ACTIVE_WORKFLOWS).join();
            if (data == null || data.length == 0) return new HashSet<>();
            return (Set<String>) KryoSerializer.fromBytes(data);
        } catch (Exception e) {
            log.warn("WorkflowDispatcher[{}]: failed to load active-workflows from KV, using empty set",
                    taskQueue, e);
            return new HashSet<>();
        }
    }

    private void persistActiveWorkflows() {
        if (taskView == null) return;
        try {
            byte[] data = KryoSerializer.toBytes(new HashSet<>(activeWorkflowIds));
            taskView.setValue(KV_ACTIVE_WORKFLOWS, data).join();
        } catch (Exception e) {
            log.warn("WorkflowDispatcher[{}]: failed to persist active-workflows to KV", taskQueue, e);
        }
    }

    private long computeLastSeqnum(LogTag historyTag, List<HistoryEvent> history) {
        if (history.isEmpty()) return 0L;
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
