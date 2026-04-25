package com.cajunsystems.boudin;

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
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class CrashRecoveryTest {

    @WorkflowInterface
    public interface SimpleWorkflow {
        @WorkflowMethod
        String run(String input);
    }

    @ActivityInterface
    public interface CountingActivities {
        @ActivityMethod
        String process(String input);
    }

    public static class SimpleWorkflowImpl implements SimpleWorkflow {
        private final CountingActivities activities =
                Workflow.newActivityStub(CountingActivities.class);

        @Override
        public String run(String input) {
            return activities.process(input);
        }
    }

    public static class CountingActivitiesImpl implements CountingActivities {
        static final AtomicInteger callCount = new AtomicInteger(0);

        @Override
        public String process(String input) {
            callCount.incrementAndGet();
            return "processed:" + input;
        }
    }

    @Test
    void workerRestartDoesNotReExecuteCompletedWorkflows() throws Exception {
        CountingActivitiesImpl.callCount.set(0);
        InMemoryPersistenceAdapter persistence = new InMemoryPersistenceAdapter();
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(persistence).build());

        // First worker: run a workflow to completion
        Worker worker1 = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker1.registerWorkflow(SimpleWorkflowImpl.class);
        worker1.registerActivities(new CountingActivitiesImpl());
        worker1.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        SimpleWorkflow stub = client.newWorkflowStub(SimpleWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("hello");
        assertThat(result).isEqualTo("processed:hello");
        assertThat(CountingActivitiesImpl.callCount.get()).isEqualTo(1);

        worker1.close();

        // Second worker: restart — should NOT re-execute the completed workflow
        Worker worker2 = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker2.registerWorkflow(SimpleWorkflowImpl.class);
        worker2.registerActivities(new CountingActivitiesImpl());
        worker2.start();

        // Give worker2 time to finish startup recovery
        Thread.sleep(200);

        // Activity should not have been called again
        assertThat(CountingActivitiesImpl.callCount.get()).isEqualTo(1);

        worker2.close();
        sharedLog.close();
    }

    @Test
    void idempotentWorkflowStartDoesNotExecuteTwice() throws Exception {
        CountingActivitiesImpl.callCount.set(0);
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());

        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker.registerWorkflow(SimpleWorkflowImpl.class);
        worker.registerActivities(new CountingActivitiesImpl());
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        String fixedId = "idempotent-test-workflow";

        // First call — starts and completes normally
        SimpleWorkflow stub1 = client.newWorkflowStub(SimpleWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").workflowId(fixedId).build());
        String r1 = stub1.run("data");
        assertThat(r1).isEqualTo("processed:data");
        assertThat(CountingActivitiesImpl.callCount.get()).isEqualTo(1);

        // Second call with same workflowId — should return existing result, not re-execute
        SimpleWorkflow stub2 = client.newWorkflowStub(SimpleWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").workflowId(fixedId).build());
        String r2 = stub2.run("data");
        assertThat(r2).isEqualTo("processed:data");
        assertThat(CountingActivitiesImpl.callCount.get()).isEqualTo(1); // still 1, not 2

        worker.close();
        sharedLog.close();
    }
}
