package com.cajunsystems.boudin.workflow;

import com.cajunsystems.boudin.activity.ActivityFailureException;
import com.cajunsystems.boudin.internal.ReplayState;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.gumbo.api.SharedLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Duration;
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
                           SharedLog sharedLog, ReplayState replayState) {
        this.workflowId = workflowId;
        this.workflowType = workflowType;
        this.taskQueue = taskQueue;
        this.sharedLog = sharedLog;
        this.replayState = replayState;
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
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        pendingActivities.put(activityId, future);
        // Virtual thread parks here — cheap because virtual threads are unmounted during join()
        return future.join();
    }

    /**
     * Called by the event dispatcher when an {@code ActivityCompleted} event arrives
     * for a pending activity. Unblocks the workflow virtual thread.
     */
    public void deliverActivityResult(String activityId, byte[] resultBytes) {
        CompletableFuture<byte[]> future = pendingActivities.remove(activityId);
        if (future != null) {
            future.complete(resultBytes);
        } else {
            log.warn("Received ActivityCompleted for unknown activityId: {} in workflow: {}",
                    activityId, workflowId);
        }
    }

    /**
     * Called by the event dispatcher when an {@code ActivityFailed} event arrives.
     * Unblocks the workflow virtual thread with an exception.
     */
    public void deliverActivityFailure(String activityId, String errorType, String message) {
        CompletableFuture<byte[]> future = pendingActivities.remove(activityId);
        if (future != null) {
            future.completeExceptionally(
                    new ActivityFailureException(activityId, errorType, message));
        }
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
    }

    /**
     * Called by the event dispatcher when a {@code TimerFired} event arrives.
     */
    public void deliverTimerFired(String timerId) {
        CompletableFuture<Void> future = pendingTimers.remove(timerId);
        if (future != null) future.complete(null);
    }
}
