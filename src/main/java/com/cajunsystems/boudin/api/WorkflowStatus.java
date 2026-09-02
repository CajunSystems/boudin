package com.cajunsystems.boudin.api;

/**
 * Lifecycle state of a workflow execution, as derived from its history.
 *
 * <p>The state is a function of the terminal event in {@code workflow-history:{workflowId}}: no
 * terminal event means {@link #RUNNING}, whatever any worker currently believes.
 */
public enum WorkflowStatus {

    /** No terminal event in history. The execution may be executing, parked, or unhosted. */
    RUNNING,

    /** History holds {@code WorkflowCompleted}. */
    COMPLETED,

    /** History holds {@code WorkflowFailed}. */
    FAILED
}
