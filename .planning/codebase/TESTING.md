# Testing

## Framework

| Tool | Version | Role |
|------|---------|------|
| JUnit Jupiter | 5.10.2 | Test runner and assertions |
| AssertJ Core | 3.25.3 | Fluent assertion API |
| Awaitility | 4.2.1 | Async polling (imported, not yet used) |

## Structure

Single integration test file: `src/test/java/com/cajunsystems/boudin/GreetingWorkflowTest.java`

Tests are end-to-end integration tests (no unit tests, no mocks). Each test gets a fresh in-memory `SharedLog` and `Worker`.

## Test Lifecycle

```java
@BeforeEach void setUp() {
    // InMemoryPersistenceAdapter — no disk I/O, fast teardown
    sharedLog = SharedLogService.open(SharedLogConfig.builder()
        .persistenceAdapter(new InMemoryPersistenceAdapter()).build());
    worker = Worker.newBuilder().taskQueue("test-queue").sharedLog(sharedLog).build();
    worker.registerWorkflow(GreetingWorkflowImpl.class);
    worker.registerWorkflow(WaitingWorkflowImpl.class);
    worker.registerActivities(new GreetingActivitiesImpl());
    worker.start();
}

@AfterEach void tearDown() {
    worker.close();
    sharedLog.close();
}
```

## Test Cases

| Test | What it covers |
|------|---------------|
| `simpleWorkflowWithActivity()` | Basic workflow → activity → result round-trip |
| `workflowCompletesBeforeSignal()` | Workflow with default state, no signal needed |
| `workflowWithSignalBeforeRun()` | Async signal → `Workflow.await()` → conditional return |
| `multipleWorkflowsRunConcurrently()` | Parallel workflow instances with isolated state |

## Async Test Pattern

```java
@Test
void workflowWithSignalBeforeRun() {
    CompletableFuture<String> resultFuture = CompletableFuture.supplyAsync(
        () -> stub.run("Monde"));

    Thread.sleep(200);  // wait for workflow to reach await()
    stub.approve("fr"); // send signal

    assertThat(resultFuture.join()).isEqualTo("Bonjour, Monde!");
}
```

## CI/CD

`.github/workflows/ci.yml`:
- Trigger: push to all branches + PRs to `main`/`master`
- Environment: Ubuntu latest, Java 21 Temurin
- Command: `mvn verify --batch-mode --no-transfer-progress`
- Maven caching enabled
- Suite timeout: 120s, per-test timeout: 30s (via Surefire config)

## Coverage Gaps

**Tested:**
- Happy-path workflow execution with activities
- Signal delivery and condition-based waiting
- Concurrent workflow isolation
- Activity result serialization/deserialization

**Not tested:**
- Activity failure and error propagation
- Workflow failure scenarios
- Crash recovery (worker restart with in-flight workflows)
- Activity retry logic
- Timer/sleep behavior
- Query methods
- Large-scale concurrency
- Timeout enforcement
- Signal replay ordering
- Schema evolution (old history replayed with new code)
