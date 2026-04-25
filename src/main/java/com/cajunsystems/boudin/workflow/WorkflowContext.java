package com.cajunsystems.boudin.workflow;

import com.cajunsystems.boudin.activity.ActivityFailureException;
import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.boudin.internal.HashedWheelTimer;
import com.cajunsystems.boudin.internal.ReplayState;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

/**
 * Per-workflow-instance execution state and thread coordination primitives.
 *
 * <p>One {@code WorkflowContext} exists for every running (or replaying) workflow instance.
 * It is attached to the workflow virtual thread via
 * {@link WorkflowThread#CURRENT_CONTEXT}, allowing static methods in
 * {@link Workflow} to access it without passing it explicitly.
 *
 * <h2>Activity waiting</h2>
 * When the workflow calls an activity via {@link com.cajunsystems.boudin.activity.ActivityStub},
 * the stub registers a {@link CompletableFuture} keyed by activityId then calls
 * {@link #awaitActivityResult}. The workflow virtual thread parks on {@code future.join()}.
 * When {@link WorkflowRunner} delivers an {@code ActivityCompleted} event from the log,
 * it calls {@link #deliverActivityResult} which completes the future and unblocks the
 * workflow thread.
 *
 * <h2>Signal waiting</h2>
 * {@link Workflow#await(BooleanSupplier)} and {@link Workflow#await(Duration, BooleanSupplier)}
 * park the workflow thread on {@link #signalLock}. Signal delivery via
 * {@link #deliverSignal} calls {@code signalLock.notifyAll()} after invoking the
 * signal method, waking the workflow thread to re-evaluate its condition.
 *
 * <h2>Thread safety</h2>
 * Activity futures are in a {@link ConcurrentHashMap} — {@code put} (workflow thread)
 * and {@code complete} (dispatcher thread) are safe to call concurrently.
 * Signal delivery is synchronized on {@link #signalLock} so the signal invocation
 * and the condition re-evaluation are mutually exclusive.
 */
public class WorkflowContext {

    private static final Logger log = LoggerFactory.getLogger(WorkflowContext.class);

    /** Immutable identity */
    public final String workflowId;
    public final String workflowType;
    public final String taskQueue;
    public final SharedLog sharedLog;

    /** Replay machinery (set at construction, read-only after that) */
    public final ReplayState replayState;

    /** Timer wheel for scheduling durable sleeps. */
    private final HashedWheelTimer timerWheel;

    /**
     * Pending activity futures: activityId → CompletableFuture&lt;byte[]&gt;.
     * Put by workflow thread, completed by dispatcher thread.
     */
    private final ConcurrentHashMap<String, CompletableFuture<byte[]>> pendingActivities =
            new ConcurrentHashMap<>();

    /**
     * Pending timer futures: timerId → CompletableFuture&lt;Void&gt;.
     */
    private final ConcurrentHashMap<String, CompletableFuture<Void>> pendingTimers =
            new ConcurrentHashMap<>();

    /**
     * Lock used for signal-based waiting. {@link Workflow#await} methods synchronize
     * on this object; {@link #deliverSignal} calls {@code notifyAll()} after applying
     * the signal, causing waiting conditions to be re-evaluated.
     */
    public final Object signalLock = new Object();

    /** Set by WorkflowThread after the virtual thread is started. */
    public volatile Thread workflowThread;

    /** CompletableFuture that is completed when the workflow method returns or throws. */
    public final CompletableFuture<byte[]> completionFuture = new CompletableFuture<>();

    public WorkflowContext(String workflowId, String workflowType, String taskQueue,
                           SharedLog sharedLog, ReplayState replayState,
                           HashedWheelTimer timerWheel) {
        this.workflowId = workflowId;
        this.workflowType = workflowType;
        this.taskQueue = taskQueue;
        this.sharedLog = sharedLog;
        this.replayState = replayState;
        this.timerWheel = timerWheel;
    }

    // ── Activity coordination ────────────────────────────────────────────────

    /**
     * Registers a pending activity future and blocks the calling (workflow) thread until
     * {@link #deliverActivityResult} or {@link #deliverActivityFailure} is called.
     *
     * @param activityId the unique activity ID
     * @return the Kryo-serialized result bytes
     * @throws ActivityFailureException if the activity failed
     */
    public byte[] awaitActivityResult(String activityId) {
        // computeIfAbsent so that if deliverActivityResult ran first and already put
        // a completed future in the map, we just join() on that immediately.
        CompletableFuture<byte[]> future =
                pendingActivities.computeIfAbsent(activityId, id -> new CompletableFuture<>());
        // Virtual thread parks here — cheap because virtual threads are unmounted during join()
        try {
            return future.join();
        } catch (java.util.concurrent.CompletionException e) {
            // Unwrap so workflow code can catch ActivityFailureException directly
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            throw e;
        } finally {
            pendingActivities.remove(activityId);
        }
    }

    /**
     * Called by the event dispatcher when an {@code ActivityCompleted} event arrives
     * for a pending activity. Unblocks the workflow virtual thread.
     */
    public void deliverActivityResult(String activityId, byte[] resultBytes) {
        // computeIfAbsent so that if awaitActivityResult hasn't registered its future yet
        // (race: activity completed before workflow thread called awaitActivityResult),
        // we pre-create the future already completed so awaitActivityResult returns immediately.
        pendingActivities.computeIfAbsent(activityId, id -> new CompletableFuture<>())
                         .complete(resultBytes);
    }

    /**
     * Called by the event dispatcher when an {@code ActivityFailed} event arrives.
     * Unblocks the workflow virtual thread with an exception.
     */
    public void deliverActivityFailure(String activityId, String errorType, String message) {
        pendingActivities.computeIfAbsent(activityId, id -> new CompletableFuture<>())
                         .completeExceptionally(
                                 new ActivityFailureException(activityId, errorType, message));
    }

    // ── Signal / condition waiting ───────────────────────────────────────────

    /**
     * Called by the event dispatcher when a {@code SignalReceived} event arrives.
     * Invokes the signal method on the workflow implementation, then wakes any
     * threads waiting in {@link Workflow#await}.
     *
     * @param signalMethod the {@link com.cajunsystems.boudin.annotation.SignalMethod}-annotated method
     * @param impl         the workflow implementation instance
     * @param payloadBytes Kryo-serialized signal arguments
     */
    public void deliverSignal(Method signalMethod, Object impl, byte[] payloadBytes) {
        synchronized (signalLock) {
            try {
                Object[] args = payloadBytes != null && payloadBytes.length > 0
                        ? (Object[]) KryoSerializer.fromBytes(payloadBytes)
                        : new Object[0];
                signalMethod.invoke(impl, args);
            } catch (Exception e) {
                log.error("Signal delivery failed for method {} in workflow {}",
                        signalMethod.getName(), workflowId, e);
            }
            // Wake all Workflow.await() condition waiters
            signalLock.notifyAll();
        }
    }

    /**
     * Parks the workflow thread until {@code condition} returns true or {@code timeout} elapses.
     *
     * @return true if the condition became true; false if the timeout elapsed first
     */
    public boolean awaitCondition(Duration timeout, BooleanSupplier condition) {
        if (condition.getAsBoolean()) return true;
        long deadlineNs = System.nanoTime() + timeout.toNanos();
        synchronized (signalLock) {
            while (!condition.getAsBoolean()) {
                long remainingNs = deadlineNs - System.nanoTime();
                if (remainingNs <= 0) return condition.getAsBoolean();
                long waitMs = remainingNs / 1_000_000;
                int waitNs = (int) (remainingNs % 1_000_000);
                try {
                    signalLock.wait(waitMs, waitNs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return condition.getAsBoolean();
                }
            }
        }
        return true;
    }

    /**
     * Parks the workflow thread indefinitely until {@code condition} returns true.
     * Woken after each signal delivery.
     */
    public void awaitCondition(BooleanSupplier condition) {
        if (condition.getAsBoolean()) return;
        synchronized (signalLock) {
            while (!condition.getAsBoolean()) {
                try {
                    signalLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    // ── Timer coordination ───────────────────────────────────────────────────

    /**
     * Registers a timer future and blocks until the timer fires.
     */
    public void awaitTimer(String timerId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingTimers.put(timerId, future);
        future.join();
        pendingTimers.remove(timerId);
    }

    /**
     * Called by the event dispatcher when a {@code TimerFired} event arrives.
     */
    public void deliverTimerFired(String timerId) {
        CompletableFuture<Void> future = pendingTimers.remove(timerId);
        if (future != null) future.complete(null);
    }

    /** Returns the set of timer IDs currently awaiting delivery. Used for cleanup on close. */
    public Set<String> pendingTimerIds() {
        return Set.copyOf(pendingTimers.keySet());
    }

    /**
     * Implements {@link com.cajunsystems.boudin.workflow.Workflow#sleep(Duration)}.
     *
     * <p>Three cases:
     * <ol>
     *   <li>Replay: timer already fired in history → return immediately</li>
     *   <li>In-progress recovery: TimerStarted in history but no TimerFired → re-schedule for remaining duration</li>
     *   <li>Live: append TimerStarted, schedule on timer wheel, park virtual thread</li>
     * </ol>
     */
    public void sleep(Duration duration) {
        int seq = replayState.nextTimerSequence(workflowId);
        String timerId = workflowId + ":timer:" + seq;

        if (replayState.hasTimerFired(timerId)) {
            return; // already fired in history — skip during replay
        }

        HistoryEvent.TimerStarted prior = replayState.getTimerStarted(timerId);
        if (prior != null) {
            // In-progress crash recovery: re-schedule for remaining duration
            long elapsedMs = Duration.between(prior.timestamp(), Instant.now()).toMillis();
            long remainingMs = Math.max(0, prior.durationMillis() - elapsedMs);
            scheduleTimer(timerId, remainingMs);
            awaitTimer(timerId);
            return;
        }

        // Live execution: persist TimerStarted, schedule, park
        LogTag historyTag = LogTag.of("workflow-history", workflowId);
        HistoryEvent.TimerStarted startedEvent = new HistoryEvent.TimerStarted(
                UUID.randomUUID().toString(), Instant.now(), workflowId, timerId, duration.toMillis());
        sharedLog.append(
                AppendRequest.to(historyTag, HistorySerializer.INSTANCE.serialize(startedEvent))
        ).join();

        scheduleTimer(timerId, duration.toMillis());
        awaitTimer(timerId);
    }

    private void scheduleTimer(String timerId, long delayMs) {
        timerWheel.schedule(timerId, delayMs, () ->
            // Timer fires on event loop thread; spawn virtual thread for the log I/O
            Thread.ofVirtual().name("boudin-timer-fire-" + timerId).start(() -> {
                LogTag historyTag = LogTag.of("workflow-history", workflowId);
                HistoryEvent.TimerFired firedEvent = new HistoryEvent.TimerFired(
                        UUID.randomUUID().toString(), Instant.now(), workflowId, timerId);
                try {
                    sharedLog.append(
                            AppendRequest.to(historyTag, HistorySerializer.INSTANCE.serialize(firedEvent))
                    ).join();
                } catch (Exception e) {
                    log.error("Failed to append TimerFired for timer {} in workflow {}",
                            timerId, workflowId, e);
                }
                // WorkflowRunner's history subscription delivers TimerFired via event loop
            })
        );
    }
}
