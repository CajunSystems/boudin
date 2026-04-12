package com.cajunsystems.boudin.workflow;

import com.cajunsystems.boudin.activity.ActivityOptions;
import com.cajunsystems.boudin.activity.ActivityStub;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.function.BooleanSupplier;

/**
 * Static facade providing workflow APIs for use inside workflow method implementations.
 *
 * <p>All methods on this class may <strong>only</strong> be called from within a
 * running workflow method (i.e., from the workflow virtual thread). Calling them
 * from outside will throw {@link IllegalStateException}.
 *
 * <h2>Activity stubs</h2>
 * <pre>{@code
 * private final MyActivities activities = Workflow.newActivityStub(MyActivities.class);
 * }</pre>
 *
 * <h2>Waiting for conditions (set by signals)</h2>
 * <pre>{@code
 * // Wait indefinitely until a signal sets this.approved = true
 * Workflow.await(() -> this.approved);
 *
 * // Wait up to 10 minutes
 * boolean approved = Workflow.await(Duration.ofMinutes(10), () -> this.approved);
 * }</pre>
 */
public final class Workflow {

    private Workflow() {}

    // ── Activity stubs ────────────────────────────────────────────────────────

    /**
     * Creates a typed proxy for the given activity interface that schedules activity
     * invocations via the shared log with default {@link ActivityOptions}.
     *
     * <p>The proxy may be stored as a field on the workflow implementation class and
     * called multiple times.
     *
     * @param activityInterface the {@link com.cajunsystems.boudin.annotation.ActivityInterface}-annotated interface
     * @return a typed proxy that dispatches calls through the Boudin activity system
     */
    public static <T> T newActivityStub(Class<T> activityInterface) {
        return newActivityStub(activityInterface, ActivityOptions.defaults());
    }

    /**
     * Creates a typed proxy for the given activity interface with custom options.
     */
    @SuppressWarnings("unchecked")
    public static <T> T newActivityStub(Class<T> activityInterface, ActivityOptions options) {
        // Do NOT capture the context here — the stub may be created in a field initializer
        // before the workflow thread starts. ActivityStub resolves the context lazily
        // on each method invocation via Workflow.currentContext().
        return (T) Proxy.newProxyInstance(
                activityInterface.getClassLoader(),
                new Class<?>[]{activityInterface},
                new ActivityStub(activityInterface, options)
        );
    }

    // ── Condition waiting ─────────────────────────────────────────────────────

    /**
     * Parks the workflow thread until {@code condition} becomes true.
     * The condition is re-evaluated after each signal delivery.
     *
     * <p>During replay, signals from the event history are applied to the workflow
     * implementation's fields before this method is called, so the condition may
     * already be true and this returns immediately.
     *
     * @param condition a supplier that reads workflow fields; must be deterministic
     */
    public static void await(BooleanSupplier condition) {
        WorkflowContext ctx = currentContext();
        if (ctx.replayState.isReplaying()) {
            // During replay, signals have already been applied to impl fields;
            // just evaluate the condition as it stands (it should be true).
            return;
        }
        ctx.awaitCondition(condition);
    }

    /**
     * Parks the workflow thread until {@code condition} becomes true or {@code timeout} elapses.
     *
     * @param timeout   maximum time to wait
     * @param condition a supplier that reads workflow fields
     * @return true if the condition became true; false if the timeout elapsed
     */
    public static boolean await(Duration timeout, BooleanSupplier condition) {
        WorkflowContext ctx = currentContext();
        if (ctx.replayState.isReplaying()) {
            // During replay, evaluate the condition immediately with current (replayed) state.
            return condition.getAsBoolean();
        }
        return ctx.awaitCondition(timeout, condition);
    }

    // ── Identity ──────────────────────────────────────────────────────────────

    /**
     * Returns the ID of the currently running workflow.
     */
    public static String getWorkflowId() {
        return currentContext().workflowId;
    }

    /**
     * Returns the task queue the current workflow is running on.
     */
    public static String getTaskQueue() {
        return currentContext().taskQueue;
    }

    // ── Package-private ───────────────────────────────────────────────────────

    /**
     * Returns the {@link WorkflowContext} for the currently executing workflow.
     * Throws {@link IllegalStateException} if called outside a workflow thread.
     */
    static WorkflowContext currentContext() {
        WorkflowContext ctx = WorkflowThread.CURRENT_CONTEXT.get();
        if (ctx == null) {
            throw new IllegalStateException(
                    "Workflow.* methods may only be called from within a workflow method. " +
                    "Current thread: " + Thread.currentThread().getName());
        }
        return ctx;
    }
}
