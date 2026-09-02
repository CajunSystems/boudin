package com.cajunsystems.boudin.api;

import com.cajunsystems.boudin.internal.WorkflowDispatcher;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.LogTag;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Client-facing entry point for starting and interacting with workflow executions.
 *
 * <p>The {@code WorkflowClient} does not need a running {@link Worker} — it only
 * requires access to the shared log. A client may reside in a different process
 * from the workers, communicating purely through the log.
 *
 * <h2>Starting a workflow</h2>
 * <pre>{@code
 * WorkflowClient client = WorkflowClient.newInstance(sharedLog);
 *
 * OrderWorkflow workflow = client.newWorkflowStub(
 *     OrderWorkflow.class,
 *     WorkflowOptions.newBuilder().taskQueue("orders").build());
 *
 * // Blocks until the workflow completes and returns its result
 * OrderResult result = workflow.processOrder(order);
 * }</pre>
 *
 * <h2>Starting without blocking</h2>
 * <pre>{@code
 * // Returns as soon as the workflow is durably started; collect the result whenever
 * WorkflowHandle<OrderResult> handle = client.start(() -> workflow.processOrder(order));
 * String id = handle.workflowId();
 * }</pre>
 *
 * <h2>Sending a signal to a running workflow</h2>
 * <pre>{@code
 * // From another thread / process, signal the workflow
 * OrderWorkflow signalStub = client.newWorkflowStub(
 *     OrderWorkflow.class,
 *     WorkflowOptions.newBuilder()
 *         .taskQueue("orders")
 *         .workflowId(existingWorkflowId)  // target the running workflow
 *         .build());
 * signalStub.cancelOrder("customer request");
 * }</pre>
 *
 * <h2>Signalling or querying without starting</h2>
 * <pre>{@code
 * WorkflowClient client = WorkflowClient.newInstance(sharedLog);
 *
 * OrderWorkflow existing = client.newWorkflowStub(
 *     OrderWorkflow.class, workflowId, "orders");
 *
 * existing.cancelOrder("customer request");  // @SignalMethod
 * String status = existing.getStatus();      // @QueryMethod
 * }</pre>
 */
public class WorkflowClient {

    private final SharedLog sharedLog;

    private WorkflowClient(SharedLog sharedLog) {
        this.sharedLog = Objects.requireNonNull(sharedLog, "sharedLog is required");
    }

    /**
     * Creates a new {@code WorkflowClient} backed by the given shared log.
     */
    public static WorkflowClient newInstance(SharedLog sharedLog) {
        return new WorkflowClient(sharedLog);
    }

    /**
     * Creates a typed workflow stub for starting a workflow and interacting with it.
     *
     * <p>Calling a {@code @WorkflowMethod} on the returned stub starts the workflow
     * and blocks until it completes. Calling a {@code @SignalMethod} appends a signal
     * to the running workflow's history.
     *
     * @param workflowInterface the {@code @WorkflowInterface}-annotated interface
     * @param options           task queue, optional workflow ID, and timeouts
     * @return a typed proxy implementing the workflow interface
     */
    @SuppressWarnings("unchecked")
    public <T> T newWorkflowStub(Class<T> workflowInterface, WorkflowOptions options) {
        WorkflowStub handler = new WorkflowStub(sharedLog, workflowInterface, options);
        return (T) Proxy.newProxyInstance(
                workflowInterface.getClassLoader(),
                new Class<?>[]{workflowInterface},
                handler
        );
    }

    /**
     * Creates a stub targeting an already-running workflow by ID, for sending signals and
     * running queries. It never starts a workflow — calling the {@code @WorkflowMethod} on the
     * returned stub throws {@link UnsupportedOperationException}.
     *
     * <p>Only a shared log reference is needed, so this works from a process that neither
     * started the workflow nor runs a worker.
     *
     * @param taskQueue the workflow's task queue (recorded for clarity; signals and queries
     *                  are routed by workflow ID)
     */
    @SuppressWarnings("unchecked")
    public <T> T newWorkflowStub(Class<T> workflowInterface, String workflowId, String taskQueue) {
        return (T) Proxy.newProxyInstance(
                workflowInterface.getClassLoader(),
                new Class<?>[]{workflowInterface},
                new SignalOnlyStub(sharedLog, workflowInterface, workflowId)
        );
    }

    // ── Async start and handles ───────────────────────────────────────────────

    /**
     * Starts a workflow without waiting for it, returning a {@link WorkflowHandle} for its
     * result.
     *
     * <p>Call the workflow method on a stub from this client inside the lambda. The call starts
     * the workflow durably and returns immediately rather than blocking, so the lambda's own
     * return value is a placeholder and must be ignored — the result comes from the handle:
     *
     * <pre>{@code
     * OrderWorkflow stub = client.newWorkflowStub(OrderWorkflow.class, options);
     * WorkflowHandle<OrderResult> handle = client.start(() -> stub.process(order));
     *
     * handle.workflowId();     // durable; hand this to another process
     * handle.getResult();      // block here, later, or never
     * }</pre>
     *
     * <p>The arguments are supplied by ordinary Java at the call site, so this one method covers
     * every workflow signature and the compiler still type-checks the call.
     *
     * <p>Use {@link #start(Runnable)} for a workflow method declared {@code void}. Nesting
     * {@code start} calls is not supported.
     *
     * @throws IllegalStateException if the lambda did not call a {@code @WorkflowMethod} on a
     *                               stub created by this client
     */
    public <T> WorkflowHandle<T> start(Supplier<T> invocation) {
        return startInternal(invocation::get);
    }

    /** Starts a {@code void} workflow method without waiting. See {@link #start(Supplier)}. */
    public WorkflowHandle<Void> start(Runnable invocation) {
        return startInternal(() -> {
            invocation.run();
            return null;
        });
    }

    private <T> WorkflowHandle<T> startInternal(Supplier<?> invocation) {
        WorkflowStub.AsyncStart pending = WorkflowStub.beginAsyncStart();
        try {
            invocation.get();
        } finally {
            WorkflowStub.endAsyncStart();
        }
        if (pending.workflowId == null) {
            throw new IllegalStateException(
                    "WorkflowClient.start(...) did not start a workflow. The lambda must call a "
                    + "@WorkflowMethod on a stub from WorkflowClient.newWorkflowStub(...) — a "
                    + "signal, a query, or an unrelated call does not start one.");
        }
        return new WorkflowHandle<>(sharedLog, pending.workflowId);
    }

    /**
     * Returns a handle to an existing execution, for a workflow started anywhere — by this
     * client, another process, or a previous run of this one.
     *
     * <p>Nothing but the workflow ID is needed. The result's type comes from the payload itself,
     * so use {@link #getHandle(String, Class)} only to shape the call site.
     */
    public WorkflowHandle<Object> getHandle(String workflowId) {
        return new WorkflowHandle<>(sharedLog, Objects.requireNonNull(workflowId, "workflowId"));
    }

    /** Typed form of {@link #getHandle(String)}. The type is not verified against the payload. */
    @SuppressWarnings("unused")
    public <T> WorkflowHandle<T> getHandle(String workflowId, Class<T> resultType) {
        return new WorkflowHandle<>(sharedLog, Objects.requireNonNull(workflowId, "workflowId"));
    }

    /**
     * Lists the workflow IDs a worker on {@code taskQueue} has started and not yet seen finish.
     *
     * <p>This reads the dispatcher's active-workflow set, so read it for what it is rather than
     * as a query index:
     * <ul>
     *   <li>it covers one task queue, not the whole log;</li>
     *   <li>a workflow whose worker died without processing its own terminal event stays listed
     *       until a worker recovers it;</li>
     *   <li>it is empty until a worker has run on that queue, even if workflows were started.</li>
     * </ul>
     * Use {@link WorkflowHandle#describe()} for the authoritative state of any single execution.
     */
    @SuppressWarnings("unchecked")
    public List<String> listWorkflows(String taskQueue) {
        Objects.requireNonNull(taskQueue, "taskQueue");
        byte[] data = sharedLog.getView(LogTag.of("workflow-tasks", taskQueue))
                .getValue(WorkflowDispatcher.KV_ACTIVE_WORKFLOWS).join();
        if (data == null || data.length == 0) return List.of();
        Set<String> ids = (Set<String>) KryoSerializer.fromBytes(data);
        return ids == null ? List.of() : List.copyOf(ids);
    }

    /**
     * Returns the underlying shared log (useful for advanced usage).
     */
    public SharedLog sharedLog() {
        return sharedLog;
    }
}
