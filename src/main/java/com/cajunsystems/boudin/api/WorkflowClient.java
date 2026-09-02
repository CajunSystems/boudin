package com.cajunsystems.boudin.api;

import com.cajunsystems.gumbo.api.SharedLog;

import java.lang.reflect.Proxy;
import java.util.Objects;

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
 * <h2>Sending a signal to a running workflow</h2>
 * <pre>{@code
 * // Start asynchronously (in a separate thread / non-blocking usage)
 * CompletableFuture.runAsync(() -> {
 *     workflow.processOrder(order); // this blocks until workflow ends
 * });
 *
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

    /**
     * Returns the underlying shared log (useful for advanced usage).
     */
    public SharedLog sharedLog() {
        return sharedLog;
    }
}
