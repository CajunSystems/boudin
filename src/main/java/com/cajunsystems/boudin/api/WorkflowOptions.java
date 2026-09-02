package com.cajunsystems.boudin.api;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration options for starting a workflow execution.
 *
 * <p>Use the builder to configure the task queue, optional workflow ID override,
 * and optional timeouts before passing to {@link WorkflowClient#newWorkflowStub}.
 *
 * <pre>{@code
 * WorkflowOptions options = WorkflowOptions.newBuilder()
 *     .taskQueue("orders")
 *     .workflowId("order-" + orderId)
 *     .workflowRunTimeout(Duration.ofHours(24))
 *     .build();
 * }</pre>
 */
public final class WorkflowOptions {

    /** Applied when the caller does not set {@link Builder#queryTimeout(Duration)}. */
    public static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofSeconds(10);

    private final String taskQueue;
    private final String workflowId;
    private final Duration workflowRunTimeout;
    private final Duration queryTimeout;

    private WorkflowOptions(Builder builder) {
        this.taskQueue = Objects.requireNonNull(builder.taskQueue, "taskQueue must not be null");
        this.workflowId = builder.workflowId;
        this.workflowRunTimeout = builder.workflowRunTimeout;
        this.queryTimeout = builder.queryTimeout != null
                ? builder.queryTimeout
                : DEFAULT_QUERY_TIMEOUT;
    }

    public String taskQueue() {
        return taskQueue;
    }

    /** Returns the explicit workflow ID, or null if one should be auto-generated. */
    public String workflowId() {
        return workflowId;
    }

    /** Returns the maximum wall-clock time a workflow run may take, or null for no limit. */
    public Duration workflowRunTimeout() {
        return workflowRunTimeout;
    }

    /**
     * Returns how long a {@code @QueryMethod} call waits for a worker to answer before
     * failing. Never null — defaults to {@link #DEFAULT_QUERY_TIMEOUT}.
     */
    public Duration queryTimeout() {
        return queryTimeout;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String taskQueue;
        private String workflowId;
        private Duration workflowRunTimeout;
        private Duration queryTimeout;

        private Builder() {}

        /** Sets the task queue that workers poll for this workflow type. Required. */
        public Builder taskQueue(String taskQueue) {
            this.taskQueue = taskQueue;
            return this;
        }

        /** Sets an explicit workflow ID (must be unique per workflow execution). */
        public Builder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }

        /** Sets the maximum wall-clock time a workflow run may take. */
        public Builder workflowRunTimeout(Duration timeout) {
            this.workflowRunTimeout = timeout;
            return this;
        }

        /**
         * Sets how long a {@code @QueryMethod} call waits for a worker to answer.
         * Defaults to {@link WorkflowOptions#DEFAULT_QUERY_TIMEOUT}.
         */
        public Builder queryTimeout(Duration timeout) {
            this.queryTimeout = timeout;
            return this;
        }

        public WorkflowOptions build() {
            return new WorkflowOptions(this);
        }
    }
}
