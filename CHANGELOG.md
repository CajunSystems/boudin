# Changelog

All notable changes to Boudin are documented here.

## [0.1.0] — 2026-04-25

Initial production-ready release. Boudin is a durable workflow framework built natively on
the [Gumbo](https://github.com/CajunSystems/gumbo) shared append-only log.

### Added

#### Core execution engine
- `BoudinEventLoop` — single-threaded NIO selector loop per worker; drives all dispatch
- `HashedWheelTimer` — timer wheel integrated into the event loop for durable sleep scheduling
- Hybrid execution model: NIO event loop for infrastructure, Java 21 virtual threads for user code
- KV-checkpointed crash recovery — `ActivityDispatcher` and `WorkflowDispatcher` resume from
  the last processed log entry; startup scan is O(new events), not O(all events)
- Idempotent workflow start — duplicate `WorkflowStarted` writes are detected and skipped via KV marker

#### Activity enforcement
- `ActivityOptions.maxAttempts` — retries failed activities up to N times
- `ActivityOptions.initialInterval` + `backoffCoefficient` — exponential backoff between retries
- `ActivityOptions.startToCloseTimeout` — kills an activity attempt after the configured duration;
  enforced via `CompletableFuture.orTimeout()` on a dedicated virtual thread executor
- `ActivityOptions.scheduleToStartTimeout` — fails the activity if not picked up within the window
- Terminal failure appends `ActivityFailed(errorType="maxAttemptsExceeded")` after all attempts;
  workflow receives `ActivityFailureException`

#### Durable timers
- `Workflow.sleep(Duration)` — appends `TimerStarted`; parks the workflow virtual thread;
  re-schedules for remaining duration on crash recovery
- `Workflow.await(Duration, BooleanSupplier)` — timed condition wait; returns false on timeout
- `TimerStarted` and `TimerFired` added to the `HistoryEvent` sealed hierarchy

#### Child workflows
- `Workflow.newChildWorkflowStub(Class)` and `Workflow.newChildWorkflowStub(Class, ChildWorkflowOptions)`
- `ChildWorkflowOptions` — optional task queue override and fixed workflow ID
- `ChildWorkflowFailureException` — thrown in parent when child workflow fails
- `ChildWorkflowStarted`, `ChildWorkflowCompleted`, `ChildWorkflowFailed` history events
- Child workflows have independent history; parent blocks until child completes
- Crash recovery: re-writes child `WorkflowStarted` if parent crashed between the two-phase write;
  `WorkflowDispatcher` deduplicates via `knownWorkflowIds`

#### Observability
- `Worker.Builder.metricsRegistry(MeterRegistry)` — opt-in Micrometer integration
- `BoudinMetrics` facade records workflow/activity counters, duration timers, and pending gauges
- SLF4J MDC populated with `workflowId`, `workflowType` on the workflow virtual thread
- SLF4J MDC populated with `activityId`, `activityType` during activity execution
- Default `SimpleMeterRegistry` used when no registry is provided (no-op externally)

#### Correctness and reliability
- Signal delivery idempotency — `WorkflowRunner` deduplicates events via `processedEventSeqnums`
- Registration validation — `WorkflowRegistry` and `ActivityRegistry` validate annotations at registration
- `CompletionException` unwrapping in `WorkflowContext.awaitActivityResult()` so workflow code can
  catch `ActivityFailureException` directly

### Dependencies
- Java 21 (virtual threads, sealed interfaces, pattern matching)
- Gumbo 0.2.0 (shared append-only log with per-tag KV store)
- Micrometer Core 1.12.5 (optional)
- SLF4J 2.0.12
- Kryo 5.x (via Gumbo transitive)
