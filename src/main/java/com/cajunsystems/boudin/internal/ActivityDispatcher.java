package com.cajunsystems.boudin.internal;

import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.api.TypedLogView;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Subscribes to the {@code activity-tasks:{taskQueue}} log tag, executes activity
 * invocations on virtual threads, and appends {@link HistoryEvent.ActivityCompleted}
 * or {@link HistoryEvent.ActivityFailed} back to the workflow's history tag.
 *
 * <h2>Startup and crash recovery</h2>
 * On {@link #start()}, reads all historical activity task events from the beginning of
 * the log. For each one that was not yet completed (as determined by scanning the
 * workflow's history), the activity is executed. This handles the case where a worker
 * crashed after scheduling the activity but before completing it.
 *
 * <h2>Idempotency</h2>
 * {@code scheduledActivityIds} tracks all activityIds seen in this JVM session.
 * This ensures each activity is executed at most once per session even if the
 * subscription delivers duplicate events.
 *
 * <h2>Concurrency</h2>
 * Each activity invocation runs on a dedicated virtual thread, allowing many activities
 * to execute concurrently without blocking the subscription delivery thread.
 */
public class ActivityDispatcher {

    private static final Logger log = LoggerFactory.getLogger(ActivityDispatcher.class);

    private final String taskQueue;
    private final SharedLog sharedLog;
    private final ActivityRegistry activityRegistry;

    /** ActivityIds processed (or in-flight) in this session — prevents double execution. */
    private final Set<String> scheduledActivityIds = ConcurrentHashMap.newKeySet();

    private SharedLog.Subscription liveSubscription;

    public ActivityDispatcher(String taskQueue, SharedLog sharedLog,
                               ActivityRegistry activityRegistry) {
        this.taskQueue = taskQueue;
        this.sharedLog = sharedLog;
        this.activityRegistry = activityRegistry;
    }

    /**
     * Starts the dispatcher:
     * <ol>
     *   <li>Reads all historical activity task events and re-executes any that
     *       were not completed (crash recovery).</li>
     *   <li>Subscribes to the tail of the task queue for new activity tasks.</li>
     * </ol>
     */
    public void start() {
        LogTag taskTag = LogTag.of("activity-tasks", taskQueue);
        TypedLogView<HistoryEvent> taskView =
                sharedLog.getTypedView(taskTag, HistorySerializer.INSTANCE);

        // Phase 1: process historical events (crash recovery)
        log.info("ActivityDispatcher[{}]: scanning historical activity tasks...", taskQueue);
        List<HistoryEvent> historical = taskView.readAll().join();
        for (HistoryEvent event : historical) {
            if (event instanceof HistoryEvent.ActivityScheduled as) {
                processActivityScheduled(as, true /* isHistorical */);
            }
        }
        log.info("ActivityDispatcher[{}]: finished historical scan ({} events)",
                taskQueue, historical.size());

        // Phase 2: subscribe to tail for new activity tasks (live mode)
        liveSubscription = taskView.subscribeTail(event -> {
            if (event instanceof HistoryEvent.ActivityScheduled as) {
                processActivityScheduled(as, false /* not historical */);
            }
        });
    }

    /** Stops subscriptions. In-flight virtual threads will finish naturally. */
    public void stop() {
        if (liveSubscription != null) {
            try { liveSubscription.close(); } catch (Exception ignored) {}
        }
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private void processActivityScheduled(HistoryEvent.ActivityScheduled as, boolean checkHistory) {
        // Skip activities for types we don't handle
        if (!activityRegistry.isRegistered(as.activityType())) {
            log.debug("ActivityDispatcher[{}]: ignoring activity {} (type {} not registered)",
                    taskQueue, as.activityId(), as.activityType());
            return;
        }

        // Idempotency: skip if already processing in this session
        if (!scheduledActivityIds.add(as.activityId())) {
            log.debug("ActivityDispatcher[{}]: skipping duplicate activity {}", taskQueue, as.activityId());
            return;
        }

        // For historical events: check if already completed to avoid re-execution
        if (checkHistory && isAlreadyCompleted(as.workflowId(), as.activityId())) {
            log.debug("ActivityDispatcher[{}]: activity {} already completed, skipping",
                    taskQueue, as.activityId());
            return;
        }

        // Execute on a dedicated virtual thread
        Thread.ofVirtual()
                .name("boudin-activity-" + as.activityId())
                .start(() -> executeActivity(as));
    }

    private void executeActivity(HistoryEvent.ActivityScheduled scheduled) {
        LogTag historyTag = LogTag.of("workflow-history", scheduled.workflowId());
        log.debug("Executing activity {} (type={}) for workflow {}",
                scheduled.activityId(), scheduled.activityType(), scheduled.workflowId());

        try {
            byte[] resultBytes = activityRegistry.invoke(scheduled.activityType(), scheduled.input());

            HistoryEvent.ActivityCompleted completed = new HistoryEvent.ActivityCompleted(
                    UUID.randomUUID().toString(),
                    Instant.now(),
                    scheduled.workflowId(),
                    scheduled.activityId(),
                    resultBytes
            );
            sharedLog.append(
                    AppendRequest.to(historyTag, HistorySerializer.INSTANCE.serialize(completed))
            ).join();
            log.debug("Activity {} completed for workflow {}",
                    scheduled.activityId(), scheduled.workflowId());

        } catch (Exception e) {
            String errorType = e.getClass().getName();
            String message = e.getMessage() != null ? e.getMessage() : "";
            log.warn("Activity {} failed for workflow {}: {}",
                    scheduled.activityId(), scheduled.workflowId(), message, e);

            HistoryEvent.ActivityFailed failed = new HistoryEvent.ActivityFailed(
                    UUID.randomUUID().toString(),
                    Instant.now(),
                    scheduled.workflowId(),
                    scheduled.activityId(),
                    errorType,
                    message
            );
            try {
                sharedLog.append(
                        AppendRequest.to(historyTag, HistorySerializer.INSTANCE.serialize(failed))
                ).join();
            } catch (Exception appendEx) {
                log.error("Failed to append ActivityFailed for activity {}",
                        scheduled.activityId(), appendEx);
            }
        }
    }

    /**
     * Checks whether the given activity already has a result in the workflow's history.
     * Used during startup to skip activities completed before the last crash.
     */
    private boolean isAlreadyCompleted(String workflowId, String activityId) {
        try {
            LogTag historyTag = LogTag.of("workflow-history", workflowId);
            TypedLogView<HistoryEvent> historyView =
                    sharedLog.getTypedView(historyTag, HistorySerializer.INSTANCE);
            List<HistoryEvent> history = historyView.readAll().join();
            return history.stream().anyMatch(
                    e -> (e instanceof HistoryEvent.ActivityCompleted ac
                            && ac.activityId().equals(activityId))
                      || (e instanceof HistoryEvent.ActivityFailed af
                            && af.activityId().equals(activityId)));
        } catch (Exception e) {
            log.warn("Could not check activity completion for {}: {}", activityId, e.getMessage());
            return false; // safer to try execution than to silently skip
        }
    }
}
