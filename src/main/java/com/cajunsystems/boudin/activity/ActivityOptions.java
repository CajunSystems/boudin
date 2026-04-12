package com.cajunsystems.boudin.activity;

import java.time.Duration;

/**
 * Configuration options for activity execution within a workflow.
 *
 * <p>Pass to {@link com.cajunsystems.boudin.workflow.Workflow#newActivityStub} to
 * customize retry policy, timeouts, and task queue routing for activities.
 *
 * <pre>{@code
 * ActivityOptions options = ActivityOptions.newBuilder()
 *     .startToCloseTimeout(Duration.ofSeconds(30))
 *     .maxAttempts(3)
 *     .build();
 * MyActivities activities = Workflow.newActivityStub(MyActivities.class, options);
 * }</pre>
 */
public final class ActivityOptions {

    private static final ActivityOptions DEFAULTS = new ActivityOptions(
            null, Duration.ofSeconds(10), 1);

    private final String taskQueue;
    private final Duration startToCloseTimeout;
    private final int maxAttempts;

    private ActivityOptions(String taskQueue, Duration startToCloseTimeout, int maxAttempts) {
        this.taskQueue = taskQueue;
        this.startToCloseTimeout = startToCloseTimeout;
        this.maxAttempts = maxAttempts;
    }

    /** Returns a sensible default: 10-second timeout, 1 attempt, workflow's own task queue. */
    public static ActivityOptions defaults() {
        return DEFAULTS;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Task queue override for this activity. Null means use the workflow's task queue.
     */
    public String taskQueue() {
        return taskQueue;
    }

    /** Maximum time from when the activity starts executing to when it must complete. */
    public Duration startToCloseTimeout() {
        return startToCloseTimeout;
    }

    /** Maximum number of execution attempts (1 = no retries). */
    public int maxAttempts() {
        return maxAttempts;
    }

    public static final class Builder {
        private String taskQueue;
        private Duration startToCloseTimeout = Duration.ofSeconds(10);
        private int maxAttempts = 1;

        private Builder() {}

        /** Override the task queue for this activity (defaults to workflow's task queue). */
        public Builder taskQueue(String taskQueue) {
            this.taskQueue = taskQueue;
            return this;
        }

        /** Set the maximum execution time per attempt. */
        public Builder startToCloseTimeout(Duration timeout) {
            this.startToCloseTimeout = timeout;
            return this;
        }

        /** Set the maximum number of retry attempts. */
        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public ActivityOptions build() {
            return new ActivityOptions(taskQueue, startToCloseTimeout, maxAttempts);
        }
    }
}
