package com.cajunsystems.boudin.api;

/**
 * Thrown when a workflow execution fails with an unhandled exception.
 *
 * <p>Wraps the original error type and message from the {@code WorkflowFailed}
 * history event so callers can inspect what went wrong without needing to
 * handle arbitrary exception types.
 */
public class WorkflowFailureException extends RuntimeException {

    private final String workflowId;
    private final String errorType;

    public WorkflowFailureException(String workflowId, String errorType, String message) {
        super("Workflow '" + workflowId + "' failed [" + errorType + "]: " + message);
        this.workflowId = workflowId;
        this.errorType = errorType;
    }

    public String workflowId() {
        return workflowId;
    }

    /** The fully-qualified class name of the original exception. */
    public String errorType() {
        return errorType;
    }
}
