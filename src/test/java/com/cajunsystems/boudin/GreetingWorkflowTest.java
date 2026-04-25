package com.cajunsystems.boudin;

import com.cajunsystems.boudin.annotation.ActivityInterface;
import com.cajunsystems.boudin.annotation.ActivityMethod;
import com.cajunsystems.boudin.annotation.SignalMethod;
import com.cajunsystems.boudin.annotation.WorkflowInterface;
import com.cajunsystems.boudin.annotation.WorkflowMethod;
import com.cajunsystems.boudin.api.WorkflowClient;
import com.cajunsystems.boudin.api.WorkflowOptions;
import com.cajunsystems.boudin.api.Worker;
import com.cajunsystems.boudin.workflow.Workflow;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.service.SharedLogService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end integration tests for the Boudin workflow framework.
 *
 * <p>Uses an in-memory Gumbo persistence adapter so no disk I/O is required.
 * Each test gets a fresh shared log and worker instance.
 */
class GreetingWorkflowTest {

    // ── Workflow & Activity definitions ───────────────────────────────────────

    @WorkflowInterface
    public interface GreetingWorkflow {
        @WorkflowMethod
        String greet(String name);

        @SignalMethod
        void setLocale(String locale);
    }

    @ActivityInterface
    public interface GreetingActivities {
        @ActivityMethod
        String formatGreeting(String name, String locale);
    }

    public static class GreetingActivitiesImpl implements GreetingActivities {
        @Override
        public String formatGreeting(String name, String locale) {
            return switch (locale) {
                case "fr" -> "Bonjour, " + name + "!";
                case "es" -> "Hola, " + name + "!";
                default   -> "Hello, " + name + "!";
            };
        }
    }

    public static class GreetingWorkflowImpl implements GreetingWorkflow {
        private volatile String locale = "en";
        private final GreetingActivities activities =
                Workflow.newActivityStub(GreetingActivities.class);

        @Override
        public String greet(String name) {
            return activities.formatGreeting(name, locale);
        }

        @Override
        public void setLocale(String locale) {
            this.locale = locale;
        }
    }

    // ── Signal-aware workflow ──────────────────────────────────────────────────

    @WorkflowInterface
    public interface WaitingWorkflow {
        @WorkflowMethod
        String run(String name);

        @SignalMethod
        void approve(String locale);
    }

    public static class WaitingWorkflowImpl implements WaitingWorkflow {
        static final AtomicBoolean reachedAwait = new AtomicBoolean(false);
        private volatile String approvedLocale = null;
        private final GreetingActivities activities =
                Workflow.newActivityStub(GreetingActivities.class);

        @Override
        public String run(String name) {
            reachedAwait.set(true);
            Workflow.await(() -> approvedLocale != null);
            return activities.formatGreeting(name, approvedLocale);
        }

        @Override
        public void approve(String locale) {
            this.approvedLocale = locale;
        }
    }

    // ── Test fixtures ─────────────────────────────────────────────────────────

    private SharedLogService sharedLog;
    private Worker worker;

    @BeforeEach
    void setUp() throws Exception {
        WaitingWorkflowImpl.reachedAwait.set(false);
        SharedLogConfig config = SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())
                .build();
        sharedLog = SharedLogService.open(config);

        worker = Worker.newBuilder()
                .taskQueue("test-queue")
                .sharedLog(sharedLog)
                .build();
        worker.registerWorkflow(GreetingWorkflowImpl.class);
        worker.registerWorkflow(WaitingWorkflowImpl.class);
        worker.registerActivities(new GreetingActivitiesImpl());
        worker.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (worker != null) worker.close();
        if (sharedLog != null) sharedLog.close();
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    @Test
    void simpleWorkflowWithActivity() {
        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        GreetingWorkflow stub = client.newWorkflowStub(
                GreetingWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test-queue").build());

        String result = stub.greet("World");

        assertThat(result).isEqualTo("Hello, World!");
    }

    @Test
    void workflowCompletesBeforeSignal() {
        WorkflowClient client = WorkflowClient.newInstance(sharedLog);
        GreetingWorkflow stub = client.newWorkflowStub(
                GreetingWorkflow.class,
                WorkflowOptions.newBuilder().taskQueue("test-queue").build());

        // Default locale is "en", so no signal needed
        String result = stub.greet("Monde");
        assertThat(result).isEqualTo("Hello, Monde!");
    }

    @Test
    void workflowWithSignalBeforeRun() {
        WorkflowClient client = WorkflowClient.newInstance(sharedLog);

        String workflowId = "greeting-" + System.nanoTime();
        WaitingWorkflow stub = client.newWorkflowStub(
                WaitingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .taskQueue("test-queue")
                        .workflowId(workflowId)
                        .build());

        // Run workflow asynchronously — it will block at Workflow.await()
        CompletableFuture<String> resultFuture = CompletableFuture.supplyAsync(
                () -> stub.run("Monde"));

        // Wait until the workflow virtual thread has started and reached Workflow.await()
        await().atMost(5, SECONDS).until(WaitingWorkflowImpl.reachedAwait::get);

        // Send the signal (locale = "fr")
        stub.approve("fr");

        // Wait for the workflow to complete
        String result = resultFuture.join();
        assertThat(result).isEqualTo("Bonjour, Monde!");
    }

    @Test
    void multipleWorkflowsRunConcurrently() throws Exception {
        WorkflowClient client = WorkflowClient.newInstance(sharedLog);

        CompletableFuture<String> f1 = CompletableFuture.supplyAsync(() -> {
            GreetingWorkflow s = client.newWorkflowStub(GreetingWorkflow.class,
                    WorkflowOptions.newBuilder().taskQueue("test-queue").build());
            return s.greet("Alice");
        });

        CompletableFuture<String> f2 = CompletableFuture.supplyAsync(() -> {
            GreetingWorkflow s = client.newWorkflowStub(GreetingWorkflow.class,
                    WorkflowOptions.newBuilder().taskQueue("test-queue").build());
            return s.greet("Bob");
        });

        String r1 = f1.get(10, TimeUnit.SECONDS);
        String r2 = f2.get(10, TimeUnit.SECONDS);

        assertThat(r1).isEqualTo("Hello, Alice!");
        assertThat(r2).isEqualTo("Hello, Bob!");
    }
}
