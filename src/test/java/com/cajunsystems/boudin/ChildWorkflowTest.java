package com.cajunsystems.boudin;

import com.cajunsystems.boudin.annotation.WorkflowInterface;
import com.cajunsystems.boudin.annotation.WorkflowMethod;
import com.cajunsystems.boudin.api.Worker;
import com.cajunsystems.boudin.api.WorkflowClient;
import com.cajunsystems.boudin.api.WorkflowOptions;
import com.cajunsystems.boudin.workflow.ChildWorkflowFailureException;
import com.cajunsystems.boudin.workflow.Workflow;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ChildWorkflowTest {

    // ── Shared fixtures ───────────────────────────────────────────────────────

    @WorkflowInterface
    public interface GreetingWorkflow {
        @WorkflowMethod
        String greet(String name);
    }

    public static class GreetingWorkflowImpl implements GreetingWorkflow {
        @Override
        public String greet(String name) {
            return "Hello, " + name + "!";
        }
    }

    @WorkflowInterface
    public interface ParentWorkflow {
        @WorkflowMethod
        String run(String input);
    }

    /** Calls child once and returns its result. */
    public static class SingleChildParentImpl implements ParentWorkflow {
        private final GreetingWorkflow child = Workflow.newChildWorkflowStub(GreetingWorkflow.class);

        @Override
        public String run(String input) {
            return child.greet(input);
        }
    }

    /** Calls the same child stub twice (different sequence numbers) and concatenates results. */
    public static class SequentialChildrenParentImpl implements ParentWorkflow {
        private final GreetingWorkflow child = Workflow.newChildWorkflowStub(GreetingWorkflow.class);

        @Override
        public String run(String input) {
            String first  = child.greet(input);
            String second = child.greet(input + "2");
            return first + "|" + second;
        }
    }

    // ── Failure fixtures ──────────────────────────────────────────────────────

    @WorkflowInterface
    public interface FailingChildWorkflow {
        @WorkflowMethod
        String run(String input);
    }

    /** Always throws, causing WorkflowFailed to be appended with errorType = class name. */
    public static class FailingChildWorkflowImpl implements FailingChildWorkflow {
        @Override
        public String run(String input) {
            throw new RuntimeException("child-boom: " + input);
        }
    }

    /** Parent that catches ChildWorkflowFailureException and returns a sentinel string. */
    public static class FailureHandlingParentImpl implements ParentWorkflow {
        private final FailingChildWorkflow child =
                Workflow.newChildWorkflowStub(FailingChildWorkflow.class);

        @Override
        public String run(String input) {
            try {
                return child.run(input);
            } catch (ChildWorkflowFailureException e) {
                return "child-failed:" + e.errorType();
            }
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    void parentInvokesChildWorkflow() throws Exception {
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());

        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker.registerWorkflow(SingleChildParentImpl.class);
        worker.registerWorkflow(GreetingWorkflowImpl.class);
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        ParentWorkflow stub = client.newWorkflowStub(ParentWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("World");

        assertThat(result).isEqualTo("Hello, World!");

        worker.close();
        sharedLog.close();
    }

    @Test
    void parentInvokesSequentialChildren() throws Exception {
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());

        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker.registerWorkflow(SequentialChildrenParentImpl.class);
        worker.registerWorkflow(GreetingWorkflowImpl.class);
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        ParentWorkflow stub = client.newWorkflowStub(ParentWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("Alice");

        // first call: greet("Alice") → "Hello, Alice!"
        // second call: greet("Alice2") → "Hello, Alice2!"
        assertThat(result).isEqualTo("Hello, Alice!|Hello, Alice2!");

        worker.close();
        sharedLog.close();
    }

    @Test
    void childWorkflowFailurePropagatesToParent() throws Exception {
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());

        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker.registerWorkflow(FailureHandlingParentImpl.class);
        worker.registerWorkflow(FailingChildWorkflowImpl.class);
        worker.start();

        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        ParentWorkflow stub = client.newWorkflowStub(ParentWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result = stub.run("test");

        // errorType is cause.getClass().getName() = "java.lang.RuntimeException"
        assertThat(result).isEqualTo("child-failed:java.lang.RuntimeException");

        worker.close();
        sharedLog.close();
    }

    @Test
    void workerRestartIsIdempotentAfterChildCompletes() throws Exception {
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());

        // Step 1: Start worker 1, run parent+child to completion
        Worker worker1 = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker1.registerWorkflow(SingleChildParentImpl.class);
        worker1.registerWorkflow(GreetingWorkflowImpl.class);
        worker1.start();

        WorkflowClient client1 = WorkflowClient.newInstance(sharedLog);
        ParentWorkflow stub1 = client1.newWorkflowStub(ParentWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result1 = stub1.run("World");
        assertThat(result1).isEqualTo("Hello, World!");

        worker1.close();

        // Step 2: Start worker 2 on the SAME shared log — the prior workflows are complete,
        // so worker 2 should not re-run them. It should be able to run new workflows normally.
        Worker worker2 = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();
        worker2.registerWorkflow(SingleChildParentImpl.class);
        worker2.registerWorkflow(GreetingWorkflowImpl.class);
        worker2.start();

        // Allow time for startup scan to complete
        Thread.sleep(200);

        // Start a fresh parent workflow on worker 2 and verify it works correctly
        WorkflowClient client2 = WorkflowClient.newInstance(sharedLog);
        ParentWorkflow stub2 = client2.newWorkflowStub(ParentWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test").build());
        String result2 = stub2.run("World2");
        assertThat(result2).isEqualTo("Hello, World2!");

        worker2.close();
        sharedLog.close();
    }
}
