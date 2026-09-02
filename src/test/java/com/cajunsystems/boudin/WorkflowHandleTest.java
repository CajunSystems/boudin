package com.cajunsystems.boudin;

import com.cajunsystems.boudin.annotation.SignalMethod;
import com.cajunsystems.boudin.annotation.WorkflowInterface;
import com.cajunsystems.boudin.annotation.WorkflowMethod;
import com.cajunsystems.boudin.api.Worker;
import com.cajunsystems.boudin.api.WorkflowClient;
import com.cajunsystems.boudin.api.WorkflowExecutionDescription;
import com.cajunsystems.boudin.api.WorkflowFailureException;
import com.cajunsystems.boudin.api.WorkflowHandle;
import com.cajunsystems.boudin.api.WorkflowOptions;
import com.cajunsystems.boudin.api.WorkflowStatus;
import com.cajunsystems.boudin.api.WorkflowTimeoutException;
import com.cajunsystems.boudin.workflow.Workflow;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Tests for async start and {@link WorkflowHandle} — starting a workflow without holding the
 * caller, and collecting its result later or from elsewhere.
 */
class WorkflowHandleTest {

    // ── Workflows under test ──────────────────────────────────────────────────

    @WorkflowInterface
    public interface ParkingWorkflow {
        @WorkflowMethod
        String run(String input);

        @SignalMethod
        void release(String suffix);
    }

    public static class ParkingWorkflowImpl implements ParkingWorkflow {
        static final AtomicBoolean reachedAwait = new AtomicBoolean(false);
        private volatile String suffix = null;

        @Override
        public String run(String input) {
            reachedAwait.set(true);
            Workflow.await(() -> suffix != null);
            return input + ":" + suffix;
        }

        @Override
        public void release(String suffix) {
            this.suffix = suffix;
        }
    }

    @WorkflowInterface
    public interface BoomWorkflow {
        @WorkflowMethod
        String run(String input);
    }

    public static class BoomWorkflowImpl implements BoomWorkflow {
        @Override
        public String run(String input) {
            throw new IllegalArgumentException("boom: " + input);
        }
    }

    /** Primitive return type — an async start must not hand back null to be unboxed. */
    @WorkflowInterface
    public interface CountingWorkflow {
        @WorkflowMethod
        int count(String input);
    }

    public static class CountingWorkflowImpl implements CountingWorkflow {
        @Override
        public int count(String input) {
            return input.length();
        }
    }

    @WorkflowInterface
    public interface VoidWorkflow {
        @WorkflowMethod
        void run(String input);
    }

    public static class VoidWorkflowImpl implements VoidWorkflow {
        static final AtomicBoolean ran = new AtomicBoolean(false);

        @Override
        public void run(String input) {
            ran.set(true);
        }
    }

    // ── Fixtures ──────────────────────────────────────────────────────────────

    private SharedLogService sharedLog;
    private Worker worker;
    private WorkflowClient client;

    @BeforeEach
    void setUp() throws Exception {
        ParkingWorkflowImpl.reachedAwait.set(false);
        VoidWorkflowImpl.ran.set(false);
        sharedLog = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())
                .build());

        worker = Worker.newBuilder().taskQueue("handles").sharedLog(sharedLog).build();
        worker.registerWorkflow(ParkingWorkflowImpl.class);
        worker.registerWorkflow(BoomWorkflowImpl.class);
        worker.registerWorkflow(CountingWorkflowImpl.class);
        worker.registerWorkflow(VoidWorkflowImpl.class);
        worker.start();

        client = WorkflowClient.newInstance(sharedLog);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (worker != null) worker.close();
        if (sharedLog != null) sharedLog.close();
    }

    private <T> T stub(Class<T> iface, String workflowId) {
        WorkflowOptions.Builder options = WorkflowOptions.newBuilder().taskQueue("handles");
        if (workflowId != null) options.workflowId(workflowId);
        return client.newWorkflowStub(iface, options.build());
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    void startReturnsWithoutWaitingForTheWorkflow() {
        ParkingWorkflow parking = stub(ParkingWorkflow.class, null);

        long start = System.nanoTime();
        WorkflowHandle<String> handle = client.start(() -> parking.run("ord"));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // The workflow parks indefinitely; start() must not have waited for it.
        assertThat(handle.workflowId()).isNotBlank();
        assertThat(elapsedMillis).isLessThan(2_000L);

        await().atMost(5, SECONDS).until(ParkingWorkflowImpl.reachedAwait::get);
        assertThat(handle.describe().status()).isEqualTo(WorkflowStatus.RUNNING);

        stub(ParkingWorkflow.class, handle.workflowId()).release("done");
        assertThat(handle.getResult()).isEqualTo("ord:done");
    }

    @Test
    void getResultReturnsImmediatelyForAnAlreadyCompletedWorkflow() {
        CountingWorkflow counting = stub(CountingWorkflow.class, null);
        WorkflowHandle<Integer> handle = client.start(() -> counting.count("abcd"));

        assertThat(handle.getResult()).isEqualTo(4);

        // Same handle again, and a fresh one, both resolve from history alone.
        assertThat(handle.getResult()).isEqualTo(4);
        assertThat(client.getHandle(handle.workflowId(), Integer.class).getResult()).isEqualTo(4);
    }

    @Test
    void resultIsRetrievableByWorkflowIdFromAnotherClient() {
        ParkingWorkflow parking = stub(ParkingWorkflow.class, null);
        WorkflowHandle<String> handle = client.start(() -> parking.run("cross"));
        String workflowId = handle.workflowId();

        await().atMost(5, SECONDS).until(ParkingWorkflowImpl.reachedAwait::get);

        // A separate client, as a different process would have — only the ID crosses over.
        WorkflowClient other = WorkflowClient.newInstance(sharedLog);
        WorkflowHandle<String> reattached = other.getHandle(workflowId, String.class);

        CompletableFuture<String> result = reattached.getResultAsync();
        other.newWorkflowStub(ParkingWorkflow.class, workflowId, "handles").release("elsewhere");

        assertThat(result.join()).isEqualTo("cross:elsewhere");
    }

    @Test
    void getResultWithTimeoutThrowsWhileStillRunning() {
        ParkingWorkflow parking = stub(ParkingWorkflow.class, null);
        WorkflowHandle<String> handle = client.start(() -> parking.run("slow"));
        await().atMost(5, SECONDS).until(ParkingWorkflowImpl.reachedAwait::get);

        assertThatThrownBy(() -> handle.getResult(Duration.ofMillis(500)))
                .isInstanceOf(WorkflowTimeoutException.class)
                .hasMessageContaining("still running");

        // The timeout is the wait's, not the workflow's — the same handle still resolves.
        stub(ParkingWorkflow.class, handle.workflowId()).release("eventually");
        assertThat(handle.getResult(Duration.ofSeconds(5))).isEqualTo("slow:eventually");
    }

    @Test
    void abandoningAnAsyncResultDoesNotBreakLaterCalls() {
        ParkingWorkflow parking = stub(ParkingWorkflow.class, null);
        WorkflowHandle<String> handle = client.start(() -> parking.run("abandon"));
        await().atMost(5, SECONDS).until(ParkingWorkflowImpl.reachedAwait::get);

        // Abandon several waiters, then complete the workflow.
        for (int i = 0; i < 3; i++) handle.getResultAsync();
        stub(ParkingWorkflow.class, handle.workflowId()).release("ok");

        assertThat(handle.getResult(Duration.ofSeconds(5))).isEqualTo("abandon:ok");
    }

    @Test
    void failedWorkflowThrowsThroughTheHandle() {
        BoomWorkflow boom = stub(BoomWorkflow.class, null);
        WorkflowHandle<String> handle = client.start(() -> boom.run("bad-input"));

        assertThatThrownBy(handle::getResult)
                .isInstanceOf(WorkflowFailureException.class)
                .extracting(e -> ((WorkflowFailureException) e).errorType())
                .isEqualTo(IllegalArgumentException.class.getName());
    }

    @Test
    void describeReportsRunningThenCompleted() {
        ParkingWorkflow parking = stub(ParkingWorkflow.class, null);
        WorkflowHandle<String> handle = client.start(() -> parking.run("desc"));
        await().atMost(5, SECONDS).until(ParkingWorkflowImpl.reachedAwait::get);

        WorkflowExecutionDescription running = handle.describe();
        assertThat(running.status()).isEqualTo(WorkflowStatus.RUNNING);
        assertThat(running.isRunning()).isTrue();
        assertThat(running.workflowType()).isEqualTo("ParkingWorkflow");
        assertThat(running.taskQueue()).isEqualTo("handles");
        assertThat(running.startedAt()).isNotNull();
        assertThat(running.closedAt()).isNull();
        assertThat(running.failure()).isNull();

        stub(ParkingWorkflow.class, handle.workflowId()).release("fin");
        handle.getResult();

        await().atMost(5, SECONDS).untilAsserted(() -> {
            WorkflowExecutionDescription done = handle.describe();
            assertThat(done.status()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(done.closedAt()).isNotNull();
            assertThat(done.historyLength()).isGreaterThan(running.historyLength());
        });
    }

    @Test
    void describeReportsFailureDetail() {
        BoomWorkflow boom = stub(BoomWorkflow.class, null);
        WorkflowHandle<String> handle = client.start(() -> boom.run("nope"));

        assertThatThrownBy(handle::getResult).isInstanceOf(WorkflowFailureException.class);

        WorkflowExecutionDescription description = handle.describe();
        assertThat(description.status()).isEqualTo(WorkflowStatus.FAILED);
        assertThat(description.closedAt()).isNotNull();
        assertThat(description.failure()).isNotNull();
        assertThat(description.failure().errorType())
                .isEqualTo(IllegalArgumentException.class.getName());
        assertThat(description.failure().message()).contains("boom: nope");
    }

    @Test
    void describeOnAnUnknownWorkflowIdFails() {
        assertThatThrownBy(() -> client.getHandle("never-existed").describe())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No history for workflow");
    }

    @Test
    void listWorkflowsContainsRunningButNotCompleted() {
        ParkingWorkflow parking = stub(ParkingWorkflow.class, null);
        WorkflowHandle<String> running = client.start(() -> parking.run("listed"));
        await().atMost(5, SECONDS).until(ParkingWorkflowImpl.reachedAwait::get);

        await().atMost(5, SECONDS).untilAsserted(() ->
                assertThat(client.listWorkflows("handles")).contains(running.workflowId()));

        stub(ParkingWorkflow.class, running.workflowId()).release("bye");
        running.getResult();

        await().atMost(5, SECONDS).untilAsserted(() ->
                assertThat(client.listWorkflows("handles")).doesNotContain(running.workflowId()));

        assertThat(client.listWorkflows("no-such-queue")).isEmpty();
    }

    // ── PR #4 review regressions ─────────────────────────────────────────────

    @Test
    void startWithAStubFromADifferentSharedLogIsRejected() throws Exception {
        // Two log services in one JVM. The async-start marker is per-thread rather than
        // per-client, so without a check clientA.start(stubFromB) would durably start the
        // workflow on B's log and hand back a handle reading A's — blocking forever.
        try (SharedLogService otherLog = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())
                .build())) {

            CountingWorkflow foreignStub = WorkflowClient.newInstance(otherLog)
                    .newWorkflowStub(CountingWorkflow.class,
                            WorkflowOptions.newBuilder().taskQueue("handles").build());

            assertThatThrownBy(() -> client.start(() -> foreignStub.count("abcd")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("different shared log");
        }
    }

    @Test
    void startCallingTwoWorkflowMethodsIsRejected() {
        CountingWorkflow first = stub(CountingWorkflow.class, null);
        CountingWorkflow second = stub(CountingWorkflow.class, null);

        // Only one handle can be returned, so the second workflow would run unobserved.
        assertThatThrownBy(() -> client.start(() -> {
            first.count("abcd");
            return second.count("efghi");
        })).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("more than one workflow");
    }

    @Test
    void nestedStartIsRejectedRatherThanBlocking() {
        CountingWorkflow outer = stub(CountingWorkflow.class, null);
        CountingWorkflow inner = stub(CountingWorkflow.class, null);

        // An inner start would clear the outer's thread-local on the way out, dropping the outer
        // lambda into blocking start — a hang inside start(). Fail fast instead.
        assertThatThrownBy(() -> client.start(() -> {
            client.start(() -> inner.count("inner"));
            return outer.count("outer");
        })).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot be nested");
    }

    @Test
    void repeatedTimedWaitsOnAParkedWorkflowStillResolve() {
        ParkingWorkflow parking = stub(ParkingWorkflow.class, null);
        WorkflowHandle<String> handle = client.start(() -> parking.run("polled"));
        await().atMost(5, SECONDS).until(ParkingWorkflowImpl.reachedAwait::get);

        // Each timed wait that expires must release its subscription; before the fix this
        // accumulated one per attempt for as long as the workflow stayed parked.
        for (int i = 0; i < 20; i++) {
            assertThatThrownBy(() -> handle.getResult(Duration.ofMillis(20)))
                    .isInstanceOf(WorkflowTimeoutException.class);
        }

        stub(ParkingWorkflow.class, handle.workflowId()).release("at-last");
        assertThat(handle.getResult(Duration.ofSeconds(5))).isEqualTo("polled:at-last");
    }

    @Test
    void listWorkflowsReportsNoneWhenTheStoredValueIsUnreadable() {
        // A worker on a different build, or a partially written value, leaves bytes this build
        // cannot decode. A public read must not leak a raw Kryo failure.
        sharedLog.getView(com.cajunsystems.gumbo.core.LogTag.of("workflow-tasks", "corrupt-queue"))
                .setValue(com.cajunsystems.boudin.internal.WorkflowDispatcher.KV_ACTIVE_WORKFLOWS,
                        new byte[]{1, 2, 3, 4, 5})
                .join();

        assertThat(client.listWorkflows("corrupt-queue")).isEmpty();
    }

    @Test
    void startWithoutCallingAWorkflowMethodIsRejected() {
        assertThatThrownBy(() -> client.start(() -> "not a workflow call"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not start a workflow");
    }

    @Test
    void primitiveReturningWorkflowStartsWithoutUnboxing() {
        CountingWorkflow counting = stub(CountingWorkflow.class, null);

        // The proxy must hand back 0 rather than null here: the lambda's return unboxes it.
        WorkflowHandle<Integer> handle = client.start(() -> counting.count("seven!!"));

        assertThat(handle.getResult()).isEqualTo(7);
    }

    @Test
    void voidWorkflowStartsViaTheRunnableOverload() {
        VoidWorkflow voidWorkflow = stub(VoidWorkflow.class, null);
        WorkflowHandle<Void> handle = client.start(() -> voidWorkflow.run("go"));

        assertThat(handle.getResult(Duration.ofSeconds(5))).isNull();
        assertThat(VoidWorkflowImpl.ran).isTrue();
        assertThat(handle.describe().status()).isEqualTo(WorkflowStatus.COMPLETED);
    }

    @Test
    void startingTheSameWorkflowIdTwiceRunsOneExecution() {
        String workflowId = "fixed-" + System.nanoTime();
        CountingWorkflow first = stub(CountingWorkflow.class, workflowId);
        CountingWorkflow second = stub(CountingWorkflow.class, workflowId);

        WorkflowHandle<Integer> h1 = client.start(() -> first.count("abcd"));
        WorkflowHandle<Integer> h2 = client.start(() -> second.count("ignored-different-input"));

        assertThat(h1.workflowId()).isEqualTo(workflowId);
        assertThat(h2.workflowId()).isEqualTo(workflowId);

        // Both resolve to the first execution's result — the second start was a no-op.
        assertThat(h1.getResult()).isEqualTo(4);
        assertThat(h2.getResult()).isEqualTo(4);
        assertThat(h1.describe().historyLength()).isEqualTo(h2.describe().historyLength());
    }
}
