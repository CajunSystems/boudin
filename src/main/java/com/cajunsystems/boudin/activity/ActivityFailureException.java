package com.cajunsystems.boudin.activity;

/**
 * Thrown inside a workflow when an activity execution fails.
 *
 * <p>Wraps the error type and message from the {@code ActivityFailed} history event.
 * Workflow code may catch this to implement compensation logic:
 *
 * <pre>{@code
 * try {
 *     PaymentResult result = activities.charge(order);
 * } catch (ActivityFailureException e) {
 *     activities.refund(order); // compensate
 *     throw e;
 * }
 * }</pre>
 */
public class ActivityFailureException extends RuntimeException {

    private final String activityId;
    private final String errorType;

    public ActivityFailureException(String activityId, String errorType, String message) {
        super("Activity '" + activityId + "' failed [" + errorType + "]: " + message);
        this.activityId = activityId;
        this.errorType = errorType;
    }

    public String activityId() {
        return activityId;
    }

    /** The fully-qualified class name of the original exception thrown by the activity. */
    public String errorType() {
        return errorType;
    }
}
