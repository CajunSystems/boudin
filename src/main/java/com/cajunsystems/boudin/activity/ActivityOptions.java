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
 *     .initialInterval(Duration.ofSeconds(2))
 *     .backoffCoefficient(2.0)
 *     .build();
 * MyActivities activities = Workflow.newActivityStub(MyActivities.class, options);
 * }</pre>
 */
public final class ActivityOptions {

    private static final ActivityOptions DEFAULTS = new ActivityOptions(
            null, Duration.ofSeconds(10), 1, Duration.ofSeconds(1), 2.0, null);

    private final String taskQueue;
    private final Duration startToCloseTimeout;
    private final int maxAttempts;
    private final Duration initialInterval;
    private final double backoffCoefficient;
    private final Duration scheduleToStartTimeout; // null = no limit

    private ActivityOptions(String taskQueue, Duration startToCloseTimeout, int maxAttempts,
                            Duration initialInterval, double backoffCoefficient,
                            Duration scheduleToStartTimeout) {
        this.taskQueue = taskQueue;
        this.startToCloseTimeout = startToCloseTimeout;
        this.maxAttempts = maxAttempts;
        this.initialInterval = initialInterval;
        this.backoffCoefficient = backoffCoefficient;
        this.scheduleToStartTimeout = scheduleToStartTimeout;
    }

    /** Returns a sensible default: 10-second timeout, 1 attempt, workflow's own task queue. */
    public static ActivityOptions defaults() {
        return DEFAULTS;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Task queue override for this activity. Null means use the workflow's task queue. */
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

    /** Delay before the first retry (attempt 1→2). Subsequent delays are multiplied by backoffCoefficient. */
    public Duration initialInterval() {
        return initialInterval;
    }

    /** Backoff multiplier: delay(n) = initialInterval × backoffCoefficient^(n-1). */
    public double backoffCoefficient() {
        return backoffCoefficient;
    }

    /**
     * Maximum time between when the activity is scheduled and when execution starts.
     * Null means no limit. Enforced by the activity dispatcher on dispatch.
     */
    public Duration scheduleToStartTimeout() {
        return scheduleToStartTimeout;
    }

    public static final class Builder {
        private String taskQueue;
        private Duration startToCloseTimeout = Duration.ofSeconds(10);
        private int maxAttempts = 1;
        private Duration initialInterval = Duration.ofSeconds(1);
        private double backoffCoefficient = 2.0;
        private Duration scheduleToStartTimeout = null;

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

        /** Set the delay before the first retry. */
        public Builder initialInterval(Duration initialInterval) {
            this.initialInterval = initialInterval;
            return this;
        }

        /** Set the exponential backoff multiplier (1.0 = constant interval). */
        public Builder backoffCoefficient(double backoffCoefficient) {
            this.backoffCoefficient = backoffCoefficient;
            return this;
        }

        /** Set the maximum time from scheduling to dispatch start. Null = no limit. */
        public Builder scheduleToStartTimeout(Duration scheduleToStartTimeout) {
            this.scheduleToStartTimeout = scheduleToStartTimeout;
            return this;
        }

        public ActivityOptions build() {
            return new ActivityOptions(taskQueue, startToCloseTimeout, maxAttempts,
                                       initialInterval, backoffCoefficient, scheduleToStartTimeout);
        }
    }
}
