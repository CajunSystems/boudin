package com.cajunsystems.boudin.internal;

import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Subscribes to the {@code activity-tasks:{taskQueue}} log tag, executes activity
 * invocations on virtual threads, and appends {@link HistoryEvent.ActivityCompleted}
 * or {@link HistoryEvent.ActivityFailed} back to the workflow's history tag.
 *
 * <h2>Startup and crash recovery</h2>
 * On {@link #start()}, reads the KV checkpoint seqnum from the {@code activity-tasks:{queue}}
 * tag. If a checkpoint exists, only events after it are scanned (fast path). If none exists,
 * the full log is scanned. The checkpoint is updated after the startup scan and after each
 * live event.
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
    private static final String KV_CHECKPOINT = "checkpoint";

    private final String taskQueue;
    private final SharedLog sharedLog;
    private final ActivityRegistry activityRegistry;

    private LogView taskView;

    /** ActivityIds processed (or in-flight) in this session — prevents double execution. */
    private final Set<String> scheduledActivityIds = ConcurrentHashMap.newKeySet();

    private SharedLog.Subscription liveSubscription;

    private final ExecutorService vtExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public ActivityDispatcher(String taskQueue, SharedLog sharedLog,
                               ActivityRegistry activityRegistry) {
        this.taskQueue = taskQueue;
        this.sharedLog = sharedLog;
        this.activityRegistry = activityRegistry;
    }

    public void start() {
        LogTag taskTag = LogTag.of("activity-tasks", taskQueue);
        taskView = sharedLog.getView(taskTag);

        long checkpoint = readCheckpoint();

        log.info("ActivityDispatcher[{}]: scanning activity tasks from seqnum {} (checkpoint)...",
                taskQueue, checkpoint);

        List<com.cajunsystems.gumbo.core.LogEntry> rawEntries =
                checkpoint == 0
                    ? taskView.readAll().join()
                    : taskView.readAfter(checkpoint).join();

        long lastSeqnum = checkpoint;
        for (com.cajunsystems.gumbo.core.LogEntry entry : rawEntries) {
            HistoryEvent event = HistorySerializer.INSTANCE.deserialize(entry.data());
            if (event instanceof HistoryEvent.ActivityScheduled as) {
                processActivityScheduled(as, true /* checkHistory */);
            }
            lastSeqnum = entry.seqnum();
        }

        if (lastSeqnum > checkpoint) {
            saveCheckpoint(lastSeqnum);
        }

        log.info("ActivityDispatcher[{}]: startup scan complete ({} events, checkpoint now {})",
                taskQueue, rawEntries.size(), lastSeqnum);

        liveSubscription = taskView.subscribeTail(entry -> {
            HistoryEvent event = HistorySerializer.INSTANCE.deserialize(entry.data());
            if (event instanceof HistoryEvent.ActivityScheduled as) {
                processActivityScheduled(as, false /* not historical */);
            }
            saveCheckpoint(entry.seqnum());
        });
    }

    /** Stops subscriptions and the timeout executor. In-flight virtual threads finish naturally. */
    public void stop() {
        if (liveSubscription != null) {
            try { liveSubscription.close(); } catch (Exception ignored) {}
        }
        vtExecutor.shutdown();
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private void processActivityScheduled(HistoryEvent.ActivityScheduled as, boolean checkHistory) {
        if (!activityRegistry.isRegistered(as.activityType())) {
            log.debug("ActivityDispatcher[{}]: ignoring activity {} (type {} not registered)",
                    taskQueue, as.activityId(), as.activityType());
            return;
        }

        if (!scheduledActivityIds.add(as.activityId())) {
            log.debug("ActivityDispatcher[{}]: skipping duplicate activity {}", taskQueue, as.activityId());
            return;
        }

        if (checkHistory && isAlreadyCompleted(as.workflowId(), as.activityId())) {
            log.debug("ActivityDispatcher[{}]: activity {} already completed, skipping",
                    taskQueue, as.activityId());
            return;
        }

        Thread.ofVirtual()
                .name("boudin-activity-" + as.activityId())
                .start(() -> executeActivity(as));
    }

    private void executeActivity(HistoryEvent.ActivityScheduled scheduled) {
        LogTag historyTag = LogTag.of("workflow-history", scheduled.workflowId());
        int maxAttempts = scheduled.maxAttempts();
        long initialIntervalMs = scheduled.initialIntervalMs();
        double backoffCoefficient = scheduled.backoffCoefficient();

        Exception lastException = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            log.debug("Executing activity {} attempt {}/{} (type={}) for workflow {}",
                    scheduled.activityId(), attempt, maxAttempts,
                    scheduled.activityType(), scheduled.workflowId());
            try {
                long timeoutMs = scheduled.startToCloseTimeoutMs();
                byte[] resultBytes = (timeoutMs > 0)
                        ? invokeWithTimeout(scheduled, timeoutMs)
                        : activityRegistry.invoke(scheduled.activityType(), scheduled.input());

                HistoryEvent.ActivityCompleted completed = new HistoryEvent.ActivityCompleted(
                        UUID.randomUUID().toString(), Instant.now(),
                        scheduled.workflowId(), scheduled.activityId(), resultBytes);
                sharedLog.append(
                        AppendRequest.to(historyTag, HistorySerializer.INSTANCE.serialize(completed))
                ).join();
                log.debug("Activity {} completed for workflow {}",
                        scheduled.activityId(), scheduled.workflowId());
                return;

            } catch (Exception e) {
                lastException = e;
                Throwable cause = (e instanceof CompletionException) ? e.getCause() : e;
                boolean isTimeout = cause instanceof TimeoutException;
                log.warn("Activity {} attempt {}/{} {} for workflow {}: {}",
                        scheduled.activityId(), attempt, maxAttempts,
                        isTimeout ? "timed out" : "failed",
                        scheduled.workflowId(),
                        isTimeout ? scheduled.startToCloseTimeoutMs() + "ms" : e.getMessage());
                if (attempt < maxAttempts) {
                    long backoffMs = (long) (initialIntervalMs * Math.pow(backoffCoefficient, attempt - 1));
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return; // Worker shutting down
                    }
                }
            }
        }

        // All attempts exhausted — append permanent failure
        String message = "Activity failed after " + maxAttempts + " attempt(s)";
        if (lastException != null && lastException.getMessage() != null) {
            message += ": " + lastException.getMessage();
        }
        log.warn("Activity {} permanently failed for workflow {} after {} attempt(s)",
                scheduled.activityId(), scheduled.workflowId(), maxAttempts);

        HistoryEvent.ActivityFailed failed = new HistoryEvent.ActivityFailed(
                UUID.randomUUID().toString(), Instant.now(),
                scheduled.workflowId(), scheduled.activityId(), "maxAttemptsExceeded", message);
        try {
            sharedLog.append(
                    AppendRequest.to(historyTag, HistorySerializer.INSTANCE.serialize(failed))
            ).join();
        } catch (Exception appendEx) {
            log.error("Failed to append ActivityFailed for activity {}", scheduled.activityId(), appendEx);
        }
    }

    private byte[] invokeWithTimeout(HistoryEvent.ActivityScheduled scheduled, long timeoutMs) {
        CompletableFuture<byte[]> future = CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return activityRegistry.invoke(scheduled.activityType(), scheduled.input());
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                vtExecutor
        ).orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) throw e; // CompletionException(TimeoutException) → handled by caller
            if (cause instanceof RuntimeException re) throw re; // unwrap activity exception
            throw e;
        }
    }

    private boolean isAlreadyCompleted(String workflowId, String activityId) {
        try {
            LogTag historyTag = LogTag.of("workflow-history", workflowId);
            List<HistoryEvent> history = sharedLog.getView(historyTag).readAll().join().stream()
                    .map(entry -> HistorySerializer.INSTANCE.deserialize(entry.data()))
                    .toList();
            return history.stream().anyMatch(
                    e -> (e instanceof HistoryEvent.ActivityCompleted ac
                            && ac.activityId().equals(activityId))
                      || (e instanceof HistoryEvent.ActivityFailed af
                            && af.activityId().equals(activityId)));
        } catch (Exception e) {
            log.warn("Could not check activity completion for {}: {}", activityId, e.getMessage());
            return false;
        }
    }

    private long readCheckpoint() {
        if (taskView == null) return 0L;
        try {
            byte[] data = taskView.getValue(KV_CHECKPOINT).join();
            if (data == null || data.length < 8) return 0L;
            return ByteBuffer.wrap(data).getLong();
        } catch (Exception e) {
            log.warn("ActivityDispatcher[{}]: failed to read checkpoint from KV, starting from 0",
                    taskQueue, e);
            return 0L;
        }
    }

    private void saveCheckpoint(long seqnum) {
        if (taskView == null) return;
        try {
            taskView.setValue(KV_CHECKPOINT, ByteBuffer.allocate(8).putLong(seqnum).array()).join();
        } catch (Exception e) {
            log.warn("ActivityDispatcher[{}]: failed to save checkpoint {} to KV", taskQueue, seqnum, e);
        }
    }
}
