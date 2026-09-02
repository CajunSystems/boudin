package com.cajunsystems.boudin.api;

/**
 * Thrown when a query against a workflow cannot be answered.
 *
 * <p>The {@link #errorType()} distinguishes the failure modes:
 * <ul>
 *   <li>{@code UnknownQuery} — no {@code @QueryMethod} with that name on the workflow
 *       interface the worker has registered</li>
 *   <li>{@code QueryTimedOut} — nothing answered within the configured query timeout, usually
 *       because the workflow has already completed or no worker is polling its task queue</li>
 *   <li>anything else — the simple name of the exception the query handler threw</li>
 * </ul>
 */
public class WorkflowQueryException extends RuntimeException {

    private final String workflowId;
    private final String queryName;
    private final String errorType;

    public WorkflowQueryException(String workflowId, String queryName,
                                  String errorType, String message) {
        super("Query '" + queryName + "' on workflow '" + workflowId
                + "' failed [" + errorType + "]: " + message);
        this.workflowId = workflowId;
        this.queryName = queryName;
        this.errorType = errorType;
    }

    public String workflowId() {
        return workflowId;
    }

    public String queryName() {
        return queryName;
    }

    /** The failure category — see the class javadoc. */
    public String errorType() {
        return errorType;
    }
}
