package com.cajunsystems.boudin.workflow;

public record ChildWorkflowOptions(
        String taskQueue,   // null = inherit parent's task queue
        String workflowId   // null = auto-generate as {parent}:child:{type}:{seq}
) {
    public static ChildWorkflowOptions defaults() {
        return new ChildWorkflowOptions(null, null);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String taskQueue;
        private String workflowId;

        public Builder taskQueue(String taskQueue) { this.taskQueue = taskQueue; return this; }
        public Builder workflowId(String workflowId) { this.workflowId = workflowId; return this; }
        public ChildWorkflowOptions build() { return new ChildWorkflowOptions(taskQueue, workflowId); }
    }
}
