package com.cajunsystems.boudin.api;

import java.time.Duration;

/**
 * Thrown by {@link WorkflowHandle#getResult(Duration)} when the workflow has not reached a
 * terminal event within the given time.
 *
 * <p>This says nothing about the workflow itself: it is still running, and the same handle can be
 * waited on again. It is distinct from {@code WorkflowOptions.workflowRunTimeout}, which is a
 * server-side limit on the execution and is enforced in Phase 11.
 */
public class WorkflowTimeoutException extends RuntimeException {

    private final String workflowId;
    private final Duration timeout;

    public WorkflowTimeoutException(String workflowId, Duration timeout) {
        super("Workflow '" + workflowId + "' did not complete within " + timeout
                + ". It is still running — the wait timed out, not the workflow.");
        this.workflowId = workflowId;
        this.timeout = timeout;
    }

    public String workflowId() {
        return workflowId;
    }

    /** The timeout that elapsed. */
    public Duration timeout() {
        return timeout;
    }
}
