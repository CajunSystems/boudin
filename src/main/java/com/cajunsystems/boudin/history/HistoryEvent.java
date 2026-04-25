package com.cajunsystems.boudin.history;

import java.time.Instant;

/**
 * Sealed hierarchy of all events that can appear in a workflow's history log.
 *
 * <p>Every state transition in the Boudin workflow engine produces a {@code HistoryEvent}
 * that is appended to the workflow's history tag ({@code workflow-history:{workflowId}})
 * in the shared log. The history is the single source of truth — all workflow state
 * is derived by reading and replaying these events.
 *
 * <p>Events are serialized to bytes via {@link HistorySerializer} (Kryo-based) before
 * being written to the Gumbo shared log. The {@code input}, {@code result}, and
 * {@code payload} byte[] fields hold inner-serialized domain objects, also via
 * {@link com.cajunsystems.boudin.serialization.KryoSerializer}.
 */
public sealed interface HistoryEvent
        permits HistoryEvent.WorkflowStarted,
                HistoryEvent.WorkflowCompleted,
                HistoryEvent.WorkflowFailed,
                HistoryEvent.ActivityScheduled,
                HistoryEvent.ActivityCompleted,
                HistoryEvent.ActivityFailed,
                HistoryEvent.SignalReceived,
                HistoryEvent.TimerStarted,
                HistoryEvent.TimerFired {

    String eventId();
    Instant timestamp();

    /**
     * A workflow has been started. Written to both {@code workflow-tasks:{taskQueue}}
     * and {@code workflow-history:{workflowId}} in a single atomic append.
     */
    record WorkflowStarted(
            String eventId,
            Instant timestamp,
            String workflowId,
            String workflowType,
            String taskQueue,
            byte[] input         // KryoSerializer.toBytes(Object[] args)
    ) implements HistoryEvent {}

    /**
     * A workflow has completed successfully. Written to {@code workflow-history:{workflowId}}.
     */
    record WorkflowCompleted(
            String eventId,
            Instant timestamp,
            String workflowId,
            byte[] result        // KryoSerializer.toBytes(returnValue); null bytes for void
    ) implements HistoryEvent {}

    /**
     * A workflow has failed with an exception.
     */
    record WorkflowFailed(
            String eventId,
            Instant timestamp,
            String workflowId,
            String errorType,
            String message
    ) implements HistoryEvent {}

    /**
     * An activity has been scheduled. Written atomically to both
     * {@code workflow-history:{workflowId}} and {@code activity-tasks:{taskQueue}}.
     */
    record ActivityScheduled(
            String eventId,
            Instant timestamp,
            String workflowId,
            String activityId,          // workflowId:activityType:sequenceNumber
            String activityType,        // "InterfaceName#methodName"
            String taskQueue,
            byte[] input,               // KryoSerializer.toBytes(Object[] args)
            int maxAttempts,
            long startToCloseTimeoutMs,
            long initialIntervalMs,
            double backoffCoefficient,
            long scheduleToStartTimeoutMs // 0 = no limit
    ) implements HistoryEvent {}

    /**
     * An activity completed successfully. Written to {@code workflow-history:{workflowId}}.
     */
    record ActivityCompleted(
            String eventId,
            Instant timestamp,
            String workflowId,
            String activityId,
            byte[] result        // KryoSerializer.toBytes(returnValue); null bytes for void
    ) implements HistoryEvent {}

    /**
     * An activity failed. Written to {@code workflow-history:{workflowId}}.
     */
    record ActivityFailed(
            String eventId,
            Instant timestamp,
            String workflowId,
            String activityId,
            String errorType,
            String message
    ) implements HistoryEvent {}

    /**
     * A signal was received by a running workflow. Written to {@code workflow-history:{workflowId}}.
     */
    record SignalReceived(
            String eventId,
            Instant timestamp,
            String workflowId,
            String signalName,
            byte[] payload       // KryoSerializer.toBytes(Object[] args)
    ) implements HistoryEvent {}

    /**
     * A durable timer was started inside a workflow via {@code Workflow.sleep()}.
     */
    record TimerStarted(
            String eventId,
            Instant timestamp,
            String workflowId,
            String timerId,
            long durationMillis
    ) implements HistoryEvent {}

    /**
     * A durable timer fired. Written to {@code workflow-history:{workflowId}}.
     */
    record TimerFired(
            String eventId,
            Instant timestamp,
            String workflowId,
            String timerId
    ) implements HistoryEvent {}
}
