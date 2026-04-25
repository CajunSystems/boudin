package com.cajunsystems.boudin.workflow;

public class ChildWorkflowFailureException extends RuntimeException {

    private final String childWorkflowId;
    private final String errorType;

    public ChildWorkflowFailureException(String childWorkflowId, String errorType, String message) {
        super("Child workflow " + childWorkflowId + " failed [" + errorType + "]: " + message);
        this.childWorkflowId = childWorkflowId;
        this.errorType = errorType;
    }

    public String childWorkflowId() { return childWorkflowId; }
    public String errorType() { return errorType; }
}
