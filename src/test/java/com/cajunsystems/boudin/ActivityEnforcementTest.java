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

class ActivityEnforcementTest {

    @ActivityInterface
    public interface FlakyActivities {
        @ActivityMethod
        String process(String input);
    }

    /**
     * Fails on the first (succeedOnAttempt - 1) calls, then succeeds.
     * succeedOnAttempt=1 → always succeeds immediately.
     * succeedOnAttempt=Integer.MAX_VALUE → always fails.
     */
    public static class FlakyActivitiesImpl implements FlakyActivities {
        final AtomicInteger callCount = new AtomicInteger(0);
        final int succeedOnAttempt;

        FlakyActivitiesImpl(int succeedOnAttempt) {
            this.succeedOnAttempt = succeedOnAttempt;
        }

        @Override
        public String process(String input) {
            int attempt = callCount.incrementAndGet();
            if (attempt < succeedOnAttempt) {
                throw new RuntimeException("Simulated failure on attempt " + attempt);
            }
            return "ok:" + input;
        }
    }

    @WorkflowInterface
    public interface RetryWorkflow {
        @WorkflowMethod
        String run(String input);
    }

    /** Retries up to 3 times with a fixed 10ms interval; catches ActivityFailureException. */
    public static class Retry3WorkflowImpl implements RetryWorkflow {
        private final FlakyActivities activities = Workflow.newActivityStub(
                FlakyActivities.class,
                ActivityOptions.newBuilder()
                        .maxAttempts(3)
                        .initialInterval(Duration.ofMillis(10))
                        .backoffCoefficient(1.0) // constant 10ms for test speed
                        .build());

        @Override
        public String run(String input) {
            try {
                return activities.process(input);
            } catch (ActivityFailureException e) {
                return "failed-after-retries";
            }
        }
    }

    /** Single attempt (default maxAttempts=1); catches ActivityFailureException. */
    public static class NoRetryWorkflowImpl implements RetryWorkflow {
        private final FlakyActivities activities = Workflow.newActivityStub(
                FlakyActivities.class,
                ActivityOptions.newBuilder().maxAttempts(1).build());

        @Override
        public String run(String input) {
            try {
                return activities.process(input);
            } catch (ActivityFailureException e) {
                return "failed-no-retry";
            }
        }
    }

    @Test
    void activitySucceedsOnSecondAttempt() throws Exception {
        FlakyActivitiesImpl impl = new FlakyActivitiesImpl(2); // fail once, then succeed
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());

        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker.registerWorkflow(Retry3WorkflowImpl.class);
        worker.registerActivities(impl);
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        RetryWorkflow stub = client.newWorkflowStub(RetryWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("hello");

        assertThat(result).isEqualTo("ok:hello");
        assertThat(impl.callCount.get()).isEqualTo(2); // tried twice

        worker.close();
        sharedLog.close();
    }

    @Test
    void exhaustedRetriesDeliversActivityFailureException() throws Exception {
        FlakyActivitiesImpl impl = new FlakyActivitiesImpl(Integer.MAX_VALUE); // always fails
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());

        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker.registerWorkflow(Retry3WorkflowImpl.class);
        worker.registerActivities(impl);
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        RetryWorkflow stub = client.newWorkflowStub(RetryWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("hello");

        assertThat(result).isEqualTo("failed-after-retries");
        assertThat(impl.callCount.get()).isEqualTo(3); // tried exactly 3 times

        worker.close();
        sharedLog.close();
    }

    @Test
    void singleAttemptFailsImmediately() throws Exception {
        FlakyActivitiesImpl impl = new FlakyActivitiesImpl(Integer.MAX_VALUE); // always fails
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());

        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker.registerWorkflow(NoRetryWorkflowImpl.class);
        worker.registerActivities(impl);
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        RetryWorkflow stub = client.newWorkflowStub(RetryWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("hello");

        assertThat(result).isEqualTo("failed-no-retry");
        assertThat(impl.callCount.get()).isEqualTo(1); // only tried once

        worker.close();
        sharedLog.close();
    }
}
