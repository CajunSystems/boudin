package com.cajunsystems.boudin;

import com.cajunsystems.boudin.activity.ActivityFailureException;
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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ActivityTimeoutTest {

    @ActivityInterface
    public interface SlowActivities {
        @ActivityMethod
        String process(String input);
    }

    /** Sleeps for {@code sleepMs} before returning, simulating a slow activity. */
    public static class SlowActivitiesImpl implements SlowActivities {
        final AtomicInteger callCount = new AtomicInteger(0);
        final long sleepMs;

        SlowActivitiesImpl(long sleepMs) {
            this.sleepMs = sleepMs;
        }

        @Override
        public String process(String input) {
            callCount.incrementAndGet();
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted", e);
            }
            return "ok:" + input;
        }
    }

    @WorkflowInterface
    public interface TimeoutWorkflow {
        @WorkflowMethod
        String run(String input);
    }

    /**
     * Single attempt with a 50ms timeout. Activity sleeps 200ms → times out →
     * workflow catches ActivityFailureException.
     */
    public static class TimeoutOnceWorkflowImpl implements TimeoutWorkflow {
        private final SlowActivities activities = Workflow.newActivityStub(
                SlowActivities.class,
                ActivityOptions.newBuilder()
                        .startToCloseTimeout(Duration.ofMillis(50))
                        .maxAttempts(1)
                        .build());

        @Override
        public String run(String input) {
            try {
                return activities.process(input);
            } catch (ActivityFailureException e) {
                return "timed-out";
            }
        }
    }

    /**
     * Three attempts with a 50ms timeout per attempt. Activity always sleeps 200ms.
     * All 3 attempts time out → workflow catches ActivityFailureException after 3 tries.
     */
    public static class TimeoutAllAttemptsWorkflowImpl implements TimeoutWorkflow {
        private final SlowActivities activities = Workflow.newActivityStub(
                SlowActivities.class,
                ActivityOptions.newBuilder()
                        .startToCloseTimeout(Duration.ofMillis(50))
                        .maxAttempts(3)
                        .initialInterval(Duration.ofMillis(1))
                        .backoffCoefficient(1.0)
                        .build());

        @Override
        public String run(String input) {
            try {
                return activities.process(input);
            } catch (ActivityFailureException e) {
                return "all-timed-out";
            }
        }
    }

    @Test
    void startToCloseTimeoutFailsSlowActivity() throws Exception {
        SlowActivitiesImpl impl = new SlowActivitiesImpl(200); // sleeps 200ms, timeout is 50ms
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());

        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker.registerWorkflow(TimeoutOnceWorkflowImpl.class);
        worker.registerActivities(impl);
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        TimeoutWorkflow stub = client.newWorkflowStub(TimeoutWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("hello");

        assertThat(result).isEqualTo("timed-out");
        assertThat(impl.callCount.get()).isEqualTo(1); // attempted once, then timed out

        worker.close();
        sharedLog.close();
    }

    @Test
    void startToCloseTimeoutRetriesAllAttempts() throws Exception {
        SlowActivitiesImpl impl = new SlowActivitiesImpl(200); // always sleeps 200ms, timeout is 50ms
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());

        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker.registerWorkflow(TimeoutAllAttemptsWorkflowImpl.class);
        worker.registerActivities(impl);
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        TimeoutWorkflow stub = client.newWorkflowStub(TimeoutWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("hello");

        assertThat(result).isEqualTo("all-timed-out");
        assertThat(impl.callCount.get()).isEqualTo(3); // all 3 attempts timed out

        worker.close();
        sharedLog.close();
    }

    @Test
    void scheduleToStartTimeoutFieldPropagatesCorrectly() {
        ActivityOptions opts = ActivityOptions.newBuilder()
                .scheduleToStartTimeout(Duration.ofSeconds(30))
                .build();
        assertThat(opts.scheduleToStartTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(opts.scheduleToStartTimeout().toMillis()).isEqualTo(30_000L);

        // Default: no schedule-to-start timeout
        ActivityOptions defaults = ActivityOptions.defaults();
        assertThat(defaults.scheduleToStartTimeout()).isNull();
    }
}
