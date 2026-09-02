package com.cajunsystems.boudin.api;

import java.time.Instant;

/**
 * A point-in-time view of one workflow execution, read from its history.
 *
 * <p>Returned by {@link WorkflowHandle#describe()}. Unlike a query, this needs no worker to be
 * running the workflow — it is derived entirely from the log, so it answers for completed and
 * unhosted executions too.
 *
 * @param workflowId    the execution's ID
 * @param workflowType  the {@code @WorkflowInterface} simple name recorded at start
 * @param taskQueue     the task queue recorded at start
 * @param status        derived from the terminal event, if any
 * @param startedAt     timestamp of {@code WorkflowStarted}
 * @param closedAt      timestamp of the terminal event, or null while {@link WorkflowStatus#RUNNING}
 * @param historyLength number of events in history when this description was taken
 * @param failure       failure detail, non-null only when {@link WorkflowStatus#FAILED}
 */
public record WorkflowExecutionDescription(
        String workflowId,
        String workflowType,
        String taskQueue,
        WorkflowStatus status,
        Instant startedAt,
        Instant closedAt,
        int historyLength,
        Failure failure
) {

    /** Error type and message copied from the {@code WorkflowFailed} event. */
    public record Failure(String errorType, String message) {}

    public boolean isRunning() {
        return status == WorkflowStatus.RUNNING;
    }
}
