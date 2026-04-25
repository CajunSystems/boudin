package com.cajunsystems.boudin;

import com.cajunsystems.boudin.activity.ActivityOptions;
import com.cajunsystems.boudin.annotation.ActivityInterface;
import com.cajunsystems.boudin.annotation.ActivityMethod;
import com.cajunsystems.boudin.annotation.WorkflowInterface;
import com.cajunsystems.boudin.annotation.WorkflowMethod;
import com.cajunsystems.boudin.api.Worker;
import com.cajunsystems.boudin.api.WorkflowClient;
import com.cajunsystems.boudin.api.WorkflowOptions;
import com.cajunsystems.boudin.workflow.Workflow;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservabilityTest {

    // ── Fixtures ─────────────────────────────────────────────────────────────

    @WorkflowInterface
    public interface GreetingWorkflow {
        @WorkflowMethod
        String greet(String name);
    }

    public static class GreetingWorkflowImpl implements GreetingWorkflow {
        @Override
        public String greet(String name) { return "Hello, " + name + "!"; }
    }

    @ActivityInterface
    public interface EchoActivities {
        @ActivityMethod
        String echo(String input);
    }

    public static class EchoActivitiesImpl implements EchoActivities {
        @Override
        public String echo(String input) { return input; }
    }

    @WorkflowInterface
    public interface EchoWorkflow {
        @WorkflowMethod
        String run(String input);
    }

    public static class EchoWorkflowImpl implements EchoWorkflow {
        private final EchoActivities activities = Workflow.newActivityStub(EchoActivities.class);

        @Override
        public String run(String input) { return activities.echo(input); }
    }

    @WorkflowInterface
    public interface FailingWorkflow {
        @WorkflowMethod
        void run();
    }

    public static class FailingWorkflowImpl implements FailingWorkflow {
        @Override
        public void run() { throw new RuntimeException("boom"); }
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    void workflowCompletedIncrementsCounters() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SharedLogService sharedLog = SharedLogService.open(
            SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());
        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog)
            .metricsRegistry(registry).build();
        worker.registerWorkflow(GreetingWorkflowImpl.class);
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        GreetingWorkflow stub = client.newWorkflowStub(GreetingWorkflow.class,
            WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.greet("World");
        assertThat(result).isEqualTo("Hello, World!");

        assertThat(registry.counter("boudin.workflow.started", "workflowType", "GreetingWorkflow").count())
            .isEqualTo(1.0);
        assertThat(registry.counter("boudin.workflow.completed", "workflowType", "GreetingWorkflow").count())
            .isEqualTo(1.0);
        assertThat(registry.timer("boudin.workflow.duration", "workflowType", "GreetingWorkflow").count())
            .isEqualTo(1L);

        worker.close();
        sharedLog.close();
    }

    @Test
    void workflowFailedIncrementsFailureCounter() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SharedLogService sharedLog = SharedLogService.open(
            SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());
        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog)
            .metricsRegistry(registry).build();
        worker.registerWorkflow(FailingWorkflowImpl.class);
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        FailingWorkflow stub = client.newWorkflowStub(FailingWorkflow.class,
            WorkflowOptions.newBuilder().taskQueue("test").build());

        // The client stub throws when the workflow fails
        assertThatThrownBy(stub::run).isInstanceOf(Exception.class);

        assertThat(registry.counter("boudin.workflow.failed", "workflowType", "FailingWorkflow").count())
            .isEqualTo(1.0);
        assertThat(registry.counter("boudin.workflow.completed", "workflowType", "FailingWorkflow").count())
            .isEqualTo(0.0);
        // Duration timer is recorded even for failed workflows
        assertThat(registry.timer("boudin.workflow.duration", "workflowType", "FailingWorkflow").count())
            .isEqualTo(1L);

        worker.close();
        sharedLog.close();
    }

    @Test
    void activityCompletedIncrementsCounters() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SharedLogService sharedLog = SharedLogService.open(
            SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());
        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog)
            .metricsRegistry(registry).build();
        worker.registerWorkflow(EchoWorkflowImpl.class);
        worker.registerActivities(new EchoActivitiesImpl());
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        EchoWorkflow stub = client.newWorkflowStub(EchoWorkflow.class,
            WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("hello");
        assertThat(result).isEqualTo("hello");

        // activityType tag is "InterfaceName#methodName"
        String activityType = "EchoActivities#echo";
        assertThat(registry.counter("boudin.activity.started", "activityType", activityType).count())
            .isEqualTo(1.0);
        assertThat(registry.counter("boudin.activity.completed", "activityType", activityType).count())
            .isEqualTo(1.0);
        assertThat(registry.timer("boudin.activity.duration", "activityType", activityType).count())
            .isEqualTo(1L);
        assertThat(registry.counter("boudin.activity.retries", "activityType", activityType).count())
            .isEqualTo(0.0);

        worker.close();
        sharedLog.close();
    }

    @Test
    void pendingWorkflowsGaugeIsZeroAfterCompletion() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SharedLogService sharedLog = SharedLogService.open(
            SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());
        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog)
            .metricsRegistry(registry).build();
        worker.registerWorkflow(GreetingWorkflowImpl.class);
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        GreetingWorkflow stub = client.newWorkflowStub(GreetingWorkflow.class,
            WorkflowOptions.newBuilder().taskQueue("test").build());
        stub.greet("World");

        // After completion, the pending workflows gauge should be 0
        double pendingValue = registry.get("boudin.worker.pending_workflows")
            .tag("taskQueue", "test").gauge().value();
        assertThat(pendingValue).isEqualTo(0.0);

        worker.close();
        sharedLog.close();
    }
}
