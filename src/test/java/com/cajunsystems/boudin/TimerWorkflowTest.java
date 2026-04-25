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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TimerWorkflowTest {

    @WorkflowInterface
    public interface SleepingWorkflow {
        @WorkflowMethod
        String run(String input);
    }

    @ActivityInterface
    public interface TimerActivities {
        @ActivityMethod
        String process(String input);
    }

    /** Sleeps 100ms then calls the activity. */
    public static class SleepThenActivityWorkflowImpl implements SleepingWorkflow {
        private final TimerActivities activities = Workflow.newActivityStub(TimerActivities.class);

        @Override
        public String run(String input) {
            Workflow.sleep(Duration.ofMillis(100));
            return activities.process(input);
        }
    }

    /** Sleeps twice (50ms each) then calls the activity. */
    public static class TwoSleepsWorkflowImpl implements SleepingWorkflow {
        private final TimerActivities activities = Workflow.newActivityStub(TimerActivities.class);

        @Override
        public String run(String input) {
            Workflow.sleep(Duration.ofMillis(50));
            Workflow.sleep(Duration.ofMillis(50));
            return activities.process(input);
        }
    }

    public static class TimerActivitiesImpl implements TimerActivities {
        static final AtomicInteger callCount = new AtomicInteger(0);

        @Override
        public String process(String input) {
            callCount.incrementAndGet();
            return "done:" + input;
        }
    }

    @Test
    void sleepCompletesAndActivityRuns() throws Exception {
        TimerActivitiesImpl.callCount.set(0);
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());

        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker.registerWorkflow(SleepThenActivityWorkflowImpl.class);
        worker.registerActivities(new TimerActivitiesImpl());
        worker.start();

        long start = System.currentTimeMillis();
        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        SleepingWorkflow stub = client.newWorkflowStub(SleepingWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("hello");

        long elapsed = System.currentTimeMillis() - start;
        assertThat(result).isEqualTo("done:hello");
        assertThat(TimerActivitiesImpl.callCount.get()).isEqualTo(1);
        assertThat(elapsed).isGreaterThanOrEqualTo(90L); // sleep actually waited

        worker.close();
        sharedLog.close();
    }

    @Test
    void workerRestartSkipsCompletedTimer() throws Exception {
        TimerActivitiesImpl.callCount.set(0);
        InMemoryPersistenceAdapter persistence = new InMemoryPersistenceAdapter();
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(persistence).build());

        // Worker1: run sleep workflow to completion
        Worker worker1 = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker1.registerWorkflow(SleepThenActivityWorkflowImpl.class);
        worker1.registerActivities(new TimerActivitiesImpl());
        worker1.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        SleepingWorkflow stub = client.newWorkflowStub(SleepingWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("world");
        assertThat(result).isEqualTo("done:world");
        assertThat(TimerActivitiesImpl.callCount.get()).isEqualTo(1);
        worker1.close();

        // Worker2: restart on same persistence — should NOT re-run sleep or activity
        Worker worker2 = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker2.registerWorkflow(SleepThenActivityWorkflowImpl.class);
        worker2.registerActivities(new TimerActivitiesImpl());
        worker2.start();

        Thread.sleep(200); // let recovery finish

        assertThat(TimerActivitiesImpl.callCount.get()).isEqualTo(1); // still 1, not 2

        worker2.close();
        sharedLog.close();
    }

    @Test
    void twoSequentialSleepsWork() throws Exception {
        TimerActivitiesImpl.callCount.set(0);
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());

        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker.registerWorkflow(TwoSleepsWorkflowImpl.class);
        worker.registerActivities(new TimerActivitiesImpl());
        worker.start();

        long start = System.currentTimeMillis();
        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        SleepingWorkflow stub = client.newWorkflowStub(SleepingWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("x");

        long elapsed = System.currentTimeMillis() - start;
        assertThat(result).isEqualTo("done:x");
        assertThat(TimerActivitiesImpl.callCount.get()).isEqualTo(1);
        assertThat(elapsed).isGreaterThanOrEqualTo(90L); // two 50ms sleeps ≈ 100ms total

        worker.close();
        sharedLog.close();
    }
}
