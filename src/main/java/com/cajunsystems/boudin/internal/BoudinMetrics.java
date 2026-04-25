package com.cajunsystems.boudin.internal;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Central metrics facade for Boudin. Wraps a Micrometer {@link MeterRegistry} and
 * exposes recording methods for all workflow and activity lifecycle events.
 *
 * <p>Constructed once per {@link com.cajunsystems.boudin.api.Worker} and threaded
 * down to each dispatcher and execution component.
 */
public class BoudinMetrics {

    private final MeterRegistry registry;
    private final String taskQueue;

    private final AtomicInteger pendingWorkflowCount = new AtomicInteger(0);
    private final AtomicInteger pendingActivityCount = new AtomicInteger(0);

    public BoudinMetrics(MeterRegistry registry, String taskQueue) {
        this.registry = registry;
        this.taskQueue = taskQueue;
        Gauge.builder("boudin.worker.pending_workflows", pendingWorkflowCount, AtomicInteger::get)
                .tag("taskQueue", taskQueue)
                .register(registry);
        Gauge.builder("boudin.worker.pending_activities", pendingActivityCount, AtomicInteger::get)
                .tag("taskQueue", taskQueue)
                .register(registry);
    }

    // ── Workflow ──────────────────────────────────────────────────────────────

    public void workflowStarted(String workflowType) {
        registry.counter("boudin.workflow.started", "workflowType", workflowType).increment();
    }

    public void workflowCompleted(String workflowType, long durationNs) {
        registry.counter("boudin.workflow.completed", "workflowType", workflowType).increment();
        registry.timer("boudin.workflow.duration", "workflowType", workflowType)
                .record(durationNs, TimeUnit.NANOSECONDS);
    }

    public void workflowFailed(String workflowType, long durationNs) {
        registry.counter("boudin.workflow.failed", "workflowType", workflowType).increment();
        registry.timer("boudin.workflow.duration", "workflowType", workflowType)
                .record(durationNs, TimeUnit.NANOSECONDS);
    }

    // ── Activity ──────────────────────────────────────────────────────────────

    public void activityStarted(String activityType) {
        registry.counter("boudin.activity.started", "activityType", activityType).increment();
    }

    public void activityCompleted(String activityType, long durationNs) {
        registry.counter("boudin.activity.completed", "activityType", activityType).increment();
        registry.timer("boudin.activity.duration", "activityType", activityType)
                .record(durationNs, TimeUnit.NANOSECONDS);
    }

    public void activityFailed(String activityType) {
        registry.counter("boudin.activity.failed", "activityType", activityType).increment();
    }

    public void activityRetried(String activityType) {
        registry.counter("boudin.activity.retries", "activityType", activityType).increment();
    }

    // ── Gauges ────────────────────────────────────────────────────────────────

    public AtomicInteger pendingWorkflows() { return pendingWorkflowCount; }

    public AtomicInteger pendingActivities() { return pendingActivityCount; }

    public MeterRegistry registry() { return registry; }
}
