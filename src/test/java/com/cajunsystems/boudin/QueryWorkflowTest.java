package com.cajunsystems.boudin;

import com.cajunsystems.boudin.annotation.ActivityInterface;
import com.cajunsystems.boudin.annotation.ActivityMethod;
import com.cajunsystems.boudin.annotation.QueryMethod;
import com.cajunsystems.boudin.annotation.SignalMethod;
import com.cajunsystems.boudin.annotation.WorkflowInterface;
import com.cajunsystems.boudin.annotation.WorkflowMethod;
import com.cajunsystems.boudin.api.Worker;
import com.cajunsystems.boudin.api.WorkflowClient;
import com.cajunsystems.boudin.api.WorkflowOptions;
import com.cajunsystems.boudin.api.WorkflowQueryException;
import com.cajunsystems.boudin.workflow.Workflow;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end tests for {@code @QueryMethod} — reading the state of a running workflow.
 *
 * <p>Uses an in-memory Gumbo persistence adapter; each test gets a fresh log and worker.
 */
class QueryWorkflowTest {

    // ── Workflow under test ───────────────────────────────────────────────────

    @WorkflowInterface
    public interface OrderWorkflow {
        @WorkflowMethod
        String process(String orderId);

        @SignalMethod
        void approve(String approver);

        @SignalMethod
        void addNote(String note);

        @QueryMethod
        String getStatus();

        @QueryMethod
        String getNote();

        @QueryMethod
        int getItemCount();

        /** Query taking an argument, to prove arg round-tripping. */
        @QueryMethod
        String getItem(int index);

        @QueryMethod(name = "boom")
        String throwingQuery();
    }

    public static class OrderWorkflowImpl implements OrderWorkflow {
        static final AtomicBoolean reachedAwait = new AtomicBoolean(false);

        private final List<String> items = List.of("widget", "gizmo", "sprocket");
        private volatile String status = "PENDING";
        private volatile String approver = null;
        private volatile String note = "none";

        @Override
        public String process(String orderId) {
            status = "AWAITING_APPROVAL";
            reachedAwait.set(true);
            Workflow.await(() -> approver != null);
            status = "APPROVED";
            return orderId + " approved by " + approver;
        }

        @Override
        public void approve(String approver) {
            this.approver = approver;
        }

        @Override
        public void addNote(String note) {
            this.note = note;
        }

        @Override
        public String getStatus() {
            return status;
        }

        @Override
        public String getNote() {
            return note;
        }

        @Override
        public int getItemCount() {
            return items.size();
        }

        @Override
        public String getItem(int index) {
            return items.get(index);
        }

        @Override
        public String throwingQuery() {
            throw new IllegalStateException("query handler blew up");
        }
    }

    // ── Inherited queries (PR #3 review) ─────────────────────────────────────

    /** Query declared on a super-interface — the worker must still resolve it. */
    public interface BaseStatusQueries {
        @QueryMethod
        String getBaseStatus();
    }

    @WorkflowInterface
    public interface InheritingWorkflow extends BaseStatusQueries {
        @WorkflowMethod
        String run(String input);

        @SignalMethod
        void finish();
    }

    public static class InheritingWorkflowImpl implements InheritingWorkflow {
        static final AtomicBoolean reachedAwait = new AtomicBoolean(false);
        private volatile boolean done = false;

        @Override
        public String run(String input) {
            reachedAwait.set(true);
            Workflow.await(() -> done);
            return input;
        }

        @Override
        public void finish() {
            this.done = true;
        }

        @Override
        public String getBaseStatus() {
            return "inherited-ok";
        }
    }

    // ── Slow handler blast radius (PR #3 review) ─────────────────────────────

    @WorkflowInterface
    public interface SlowQueryWorkflow {
        @WorkflowMethod
        String run(String input);

        @SignalMethod
        void poke(String note);

        @QueryMethod
        String slowStatus();
    }

    public static class SlowQueryWorkflowImpl implements SlowQueryWorkflow {
        static final AtomicBoolean reachedAwait = new AtomicBoolean(false);
        /** Set once the handler is running, i.e. once it holds signalLock. */
        static final AtomicBoolean handlerEntered = new AtomicBoolean(false);
        static final long HANDLER_MILLIS = 3_000;
        private volatile boolean done = false;

        @Override
        public String run(String input) {
            reachedAwait.set(true);
            Workflow.await(() -> done);
            return input;
        }

        @Override
        public void poke(String note) {
            // Deliberately does not unblock run() — its only job is to make the worker
            // attempt a signal delivery while slowStatus() holds signalLock.
        }

        @Override
        public String slowStatus() {
            handlerEntered.set(true);
            try {
                Thread.sleep(HANDLER_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "slow";
        }
    }

    /**
     * Unrelated workflow used to prove other workflows still make progress.
     *
     * <p>It must call an activity: activity result delivery is what routes through
     * {@code BoudinEventLoop}, so a workflow that merely returns a value would complete without
     * ever touching the shared thread and would not detect a stall at all.
     */
    @ActivityInterface
    public interface EchoActivities {
        @ActivityMethod
        String format(String input);
    }

    public static class EchoActivitiesImpl implements EchoActivities {
        @Override
        public String format(String input) {
            return "echo:" + input;
        }
    }

    @WorkflowInterface
    public interface EchoWorkflow {
        @WorkflowMethod
        String echo(String input);
    }

    public static class EchoWorkflowImpl implements EchoWorkflow {
        private final EchoActivities activities = Workflow.newActivityStub(EchoActivities.class);

        @Override
        public String echo(String input) {
            return activities.format(input);
        }
    }

    /** Invalid: a query method that returns nothing. Registration must reject it. */
    @WorkflowInterface
    public interface VoidQueryWorkflow {
        @WorkflowMethod
        String run(String input);

        @QueryMethod
        void getStatus();
    }

    public static class VoidQueryWorkflowImpl implements VoidQueryWorkflow {
        @Override
        public String run(String input) {
            return input;
        }

        @Override
        public void getStatus() {
        }
    }

    // ── Fixtures ──────────────────────────────────────────────────────────────

    private SharedLogService sharedLog;
    private Worker worker;

    @BeforeEach
    void setUp() throws Exception {
        OrderWorkflowImpl.reachedAwait.set(false);
        InheritingWorkflowImpl.reachedAwait.set(false);
        SlowQueryWorkflowImpl.reachedAwait.set(false);
        SlowQueryWorkflowImpl.handlerEntered.set(false);
        sharedLog = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())
                .build());

        worker = Worker.newBuilder()
                .taskQueue("query-queue")
                .sharedLog(sharedLog)
                .build();
        worker.registerWorkflow(OrderWorkflowImpl.class);
        worker.registerWorkflow(InheritingWorkflowImpl.class);
        worker.registerWorkflow(SlowQueryWorkflowImpl.class);
        worker.registerWorkflow(EchoWorkflowImpl.class);
        worker.registerActivities(new EchoActivitiesImpl());
        worker.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (worker != null) worker.close();
        if (sharedLog != null) sharedLog.close();
    }

    /**
     * Starts the order workflow asynchronously and blocks until it has parked in
     * {@code Workflow.await()}, so queries hit a genuinely in-flight execution.
     */
    private CompletableFuture<String> startAndAwaitParked(OrderWorkflow stub, String orderId) {
        CompletableFuture<String> result = CompletableFuture.supplyAsync(() -> stub.process(orderId));
        await().atMost(5, SECONDS).until(OrderWorkflowImpl.reachedAwait::get);
        return result;
    }

    private OrderWorkflow newStub(String workflowId) {
        return WorkflowClient.newInstance(sharedLog).newWorkflowStub(
                OrderWorkflow.class,
                WorkflowOptions.newBuilder()
                        .taskQueue("query-queue")
                        .workflowId(workflowId)
                        .build());
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    void queryReturnsLiveStateOfRunningWorkflow() {
        OrderWorkflow stub = newStub("order-live-" + System.nanoTime());
        CompletableFuture<String> result = startAndAwaitParked(stub, "ord-1");

        assertThat(stub.getStatus()).isEqualTo("AWAITING_APPROVAL");
        assertThat(stub.getItemCount()).isEqualTo(3);

        stub.approve("alice");
        assertThat(result.join()).isEqualTo("ord-1 approved by alice");
    }

    @Test
    void queryReflectsStateChangedBySignal() {
        OrderWorkflow stub = newStub("order-signal-" + System.nanoTime());
        CompletableFuture<String> result = startAndAwaitParked(stub, "ord-2");

        assertThat(stub.getNote()).isEqualTo("none");

        // A signal that does not unblock the workflow — it stays parked, so the query
        // observes the mutation while the execution is still in flight.
        stub.addNote("expedite");

        await().atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(stub.getNote()).isEqualTo("expedite"));

        stub.approve("bob");
        assertThat(result.join()).isEqualTo("ord-2 approved by bob");
    }

    @Test
    void queryAgainstCompletedWorkflowTimesOut() {
        // Once a workflow completes, its runner (and query subscription) is torn down.
        // Reading terminal state is Phase 10's WorkflowHandle.describe(), not a query.
        String workflowId = "order-completed-" + System.nanoTime();
        OrderWorkflow stub = newStub(workflowId);
        CompletableFuture<String> result = startAndAwaitParked(stub, "ord-7");
        stub.approve("grace");
        result.join();

        OrderWorkflow observer = WorkflowClient.newInstance(sharedLog).newWorkflowStub(
                OrderWorkflow.class,
                WorkflowOptions.newBuilder()
                        .taskQueue("query-queue")
                        .workflowId(workflowId)
                        .queryTimeout(Duration.ofMillis(500))
                        .build());

        assertThatThrownBy(observer::getStatus)
                .isInstanceOf(WorkflowQueryException.class)
                .extracting(e -> ((WorkflowQueryException) e).errorType())
                .isEqualTo("QueryTimedOut");
    }

    @Test
    void queryWithArgumentRoundTrips() {
        OrderWorkflow stub = newStub("order-args-" + System.nanoTime());
        CompletableFuture<String> result = startAndAwaitParked(stub, "ord-3");

        assertThat(stub.getItem(0)).isEqualTo("widget");
        assertThat(stub.getItem(2)).isEqualTo("sprocket");

        stub.approve("carol");
        result.join();
    }

    @Test
    void queryWorksFromStubThatDidNotStartTheWorkflow() {
        String workflowId = "order-crossproc-" + System.nanoTime();
        OrderWorkflow starter = newStub(workflowId);
        CompletableFuture<String> result = startAndAwaitParked(starter, "ord-4");

        // A stub built only from the workflow ID — the cross-process path.
        OrderWorkflow observer = WorkflowClient.newInstance(sharedLog)
                .newWorkflowStub(OrderWorkflow.class, workflowId, "query-queue");

        assertThat(observer.getStatus()).isEqualTo("AWAITING_APPROVAL");
        assertThat(observer.getItemCount()).isEqualTo(3);

        observer.approve("dave");
        assertThat(result.join()).isEqualTo("ord-4 approved by dave");
    }

    @Test
    void queryHandlerThatThrowsSurfacesAsQueryException() {
        OrderWorkflow stub = newStub("order-throw-" + System.nanoTime());
        CompletableFuture<String> result = startAndAwaitParked(stub, "ord-5");

        assertThatThrownBy(stub::throwingQuery)
                .isInstanceOf(WorkflowQueryException.class)
                .hasMessageContaining("boom")
                .hasMessageContaining("query handler blew up")
                .extracting(e -> ((WorkflowQueryException) e).errorType())
                .isEqualTo("IllegalStateException");

        stub.approve("erin");
        result.join();
    }

    @Test
    void queryAgainstWorkflowNoWorkerIsRunningTimesOut() {
        OrderWorkflow stub = WorkflowClient.newInstance(sharedLog).newWorkflowStub(
                OrderWorkflow.class,
                WorkflowOptions.newBuilder()
                        .taskQueue("query-queue")
                        .workflowId("never-started-" + System.nanoTime())
                        .queryTimeout(Duration.ofMillis(500))
                        .build());

        assertThatThrownBy(stub::getStatus)
                .isInstanceOf(WorkflowQueryException.class)
                .extracting(e -> ((WorkflowQueryException) e).errorType())
                .isEqualTo("QueryTimedOut");
    }

    // ── PR #3 review regressions ─────────────────────────────────────────────

    @Test
    void queryInheritedFromSuperInterfaceIsAnswered() {
        InheritingWorkflow stub = WorkflowClient.newInstance(sharedLog).newWorkflowStub(
                InheritingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .taskQueue("query-queue")
                        .workflowId("order-inherit-" + System.nanoTime())
                        .build());

        CompletableFuture<String> result = CompletableFuture.supplyAsync(() -> stub.run("ord-8"));
        await().atMost(5, SECONDS).until(InheritingWorkflowImpl.reachedAwait::get);

        // The client proxy reads the annotation off the invoked method and does not care where
        // it was declared; the worker must resolve it the same way.
        assertThat(stub.getBaseStatus()).isEqualTo("inherited-ok");

        stub.finish();
        assertThat(result.join()).isEqualTo("ord-8");
    }

    @Test
    void slowQueryHandlerDoesNotStallOtherWorkflowsOnTheWorker() throws Exception {
        SlowQueryWorkflow slow = WorkflowClient.newInstance(sharedLog).newWorkflowStub(
                SlowQueryWorkflow.class,
                WorkflowOptions.newBuilder()
                        .taskQueue("query-queue")
                        .workflowId("order-slow-" + System.nanoTime())
                        .queryTimeout(Duration.ofSeconds(10))
                        .build());

        CompletableFuture<String> slowRun = CompletableFuture.supplyAsync(() -> slow.run("ord-9"));
        await().atMost(5, SECONDS).until(SlowQueryWorkflowImpl.reachedAwait::get);

        // Hold signalLock for HANDLER_MILLIS, then make the worker try to deliver a signal to
        // this workflow while it is held. If signal delivery ran on the shared event loop, that
        // single platform thread would now be blocked for every workflow on the worker.
        CompletableFuture<String> slowQuery = CompletableFuture.supplyAsync(slow::slowStatus);

        // Wait until the handler is actually running before signalling: supplyAsync returns
        // immediately, so poking first would race the handler and might find signalLock free.
        await().atMost(5, SECONDS).until(SlowQueryWorkflowImpl.handlerEntered::get);
        slow.poke("while-locked");

        // Let the signal delivery reach the worker and block on signalLock, which is the state
        // being tested. Without the per-workflow signal thread this is the moment the shared
        // event loop goes dead for every workflow on the queue.
        Thread.sleep(300);

        // An unrelated workflow must still complete well inside the handler's duration.
        // Its activity result is delivered via the shared event loop, which is the
        // thread a blocked signal delivery would have been occupying.
        long start = System.nanoTime();
        EchoWorkflow echo = WorkflowClient.newInstance(sharedLog).newWorkflowStub(
                EchoWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("query-queue").build());
        assertThat(echo.echo("hi")).isEqualTo("echo:hi");
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // Generous margin, but far below the handler's remaining ~2.7s: a stalled event loop
        // cannot deliver the echo activity's result until the handler releases the lock.
        assertThat(elapsedMillis)
                .as("unrelated workflow blocked behind the slow query handler")
                .isLessThan(1_000L);

        assertThat(slowQuery.join()).isEqualTo("slow");
        slowRun.cancel(true);
    }

    @Test
    void workerRestartDoesNotReanswerHistoricalQueries() throws Exception {
        String workflowId = "order-restart-" + System.nanoTime();
        OrderWorkflow stub = newStub(workflowId);
        CompletableFuture<String> result = startAndAwaitParked(stub, "ord-10");

        stub.getStatus();
        stub.getItemCount();

        int responsesBefore = countQueryResponses(workflowId);
        assertThat(responsesBefore).isEqualTo(2);

        // Second worker on the same log and queue — the shape a restart takes. Subscribing from
        // BEGINNING made it re-invoke every handler and append a duplicate response for every
        // request ever made.
        Worker restarted = Worker.newBuilder()
                .taskQueue("query-queue")
                .sharedLog(sharedLog)
                .build();
        restarted.registerWorkflow(OrderWorkflowImpl.class);
        restarted.start();
        try {
            Thread.sleep(500);
            assertThat(countQueryResponses(workflowId)).isEqualTo(responsesBefore);
        } finally {
            restarted.close();
        }

        stub.approve("heidi");
        result.join();
    }

    /** Counts QueryCompleted/QueryFailed entries on a workflow's query tag. */
    private int countQueryResponses(String workflowId) {
        return (int) sharedLog
                .getView(com.cajunsystems.gumbo.core.LogTag.of("workflow-queries", workflowId))
                .readAll().join().stream()
                .map(entry -> com.cajunsystems.boudin.query.QuerySerializer.INSTANCE
                        .deserialize(entry.data()))
                .filter(m -> !(m instanceof com.cajunsystems.boudin.query.QueryMessage.QueryRequested))
                .count();
    }

    @Test
    void voidReturningQueryMethodIsRejectedAtRegistration() {
        assertThatThrownBy(() -> worker.registerWorkflow(VoidQueryWorkflowImpl.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("getStatus")
                .hasMessageContaining("returns void");
    }

    @Test
    void queryDoesNotAppearInWorkflowHistory() {
        String workflowId = "order-history-" + System.nanoTime();
        OrderWorkflow stub = newStub(workflowId);
        CompletableFuture<String> result = startAndAwaitParked(stub, "ord-6");

        stub.getStatus();
        stub.getItemCount();

        stub.approve("frank");
        result.join();

        // The history tag must contain no query traffic — queries live on their own tag so
        // replay and determinism are untouched.
        List<String> historyTypes = sharedLog
                .getView(com.cajunsystems.gumbo.core.LogTag.of("workflow-history", workflowId))
                .readAll().join().stream()
                .map(entry -> com.cajunsystems.boudin.history.HistorySerializer.INSTANCE
                        .deserialize(entry.data()).getClass().getSimpleName())
                .toList();

        assertThat(historyTypes).isNotEmpty();
        assertThat(historyTypes).noneMatch(t -> t.startsWith("Query"));
    }
}
