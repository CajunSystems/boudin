# Boudin Workflow Framework — Roadmap

## Milestone 1: Production-Ready Boudin

Evolve Boudin from a proof-of-concept into a production-grade durable workflow engine built natively on Gumbo. Reliability first, then the NIO event loop as the new execution engine, then features layered on top.

---

### Phase 1: Foundation & Correctness

**Goal:** Fix existing correctness bugs before building anything new. A reliable foundation is non-negotiable.

**Scope:**
- Signal delivery idempotency — deduplicate `SignalReceived` events so duplicate delivery doesn't fire handler twice
- Pending activities map pruning — remove completed futures from `WorkflowContext.pendingActivities` to prevent memory accumulation
- Registration validation — validate at `registerWorkflow`/`registerActivities` that exactly one `@WorkflowMethod` exists, no duplicate names, required annotations present
- Test reliability — replace `Thread.sleep(200)` with Awaitility-based polling in `GreetingWorkflowTest`
- Error message quality — clear messages when workflow APIs called outside workflow thread

**Exit criteria:** All existing tests pass; no known correctness bugs in current feature set.

---

### Phase 2: Scalable Crash Recovery

**Goal:** Replace the unbounded startup log scan with KV-checkpointed recovery. Workers resume from where they left off.

**Scope:**
- Per-worker KV checkpoints — use Gumbo 0.2.0 `LogView.setValue()` to persist last-processed seqnum per task queue tag
- `readAfter(checkpoint)` on startup — replace `readAll()` with positional read from checkpoint
- `WorkflowDispatcher` and `ActivityDispatcher` startup rewrite — checkpoint-aware recovery
- Idempotent workflow start — use KV to record started workflow IDs; double-start returns or no-ops instead of creating duplicate execution
- Test: verify recovery from a checkpoint (worker restart after partial execution)

**Exit criteria:** Worker restart scans only new events since last checkpoint; idempotent workflow start safe.

---

### Phase 3: NIO Event Loop Infrastructure

**Goal:** Replace subscription-callback dispatchers with a selector-based event loop. This is the new execution engine for all of Boudin's infrastructure.

**Scope:**
- `BoudinEventLoop` — single-threaded NIO selector loop per worker; drives dispatch, delivers events, schedules timers
- `HashedWheelTimer` — timer wheel integrated into the event loop; fires `TimerExpired` callbacks at scheduled times
- Migrate `WorkflowDispatcher` to event loop — receives log events via loop instead of inline callbacks
- Migrate `ActivityDispatcher` to event loop — activity task delivery via loop
- Virtual threads preserved for user code — workflow methods and activity impls still run on virtual threads, launched by the event loop
- `WorkflowRunner` event delivery refactored — history events arrive via loop dispatch, not direct callback
- Shutdown sequencing — clean event loop shutdown drains pending events before stopping

**Exit criteria:** All existing tests pass with the new event loop; no behavioral change from user perspective.

---

### Phase 4: Timers & Workflow.sleep()

**Goal:** Implement `Workflow.sleep()` and timeout-aware `Workflow.await()` using the Phase 3 timer wheel.

**Scope:**
- `Workflow.sleep(Duration)` — appends `TimerStarted` to history; parks workflow virtual thread; event loop fires `TimerFired` when due
- `Workflow.await(Duration, BooleanSupplier)` — returns false on timeout, true if condition met first
- `WorkflowOptions.workflowRunTimeout` enforcement — workflow killed with `WorkflowTimedOut` failure if it exceeds the configured duration
- Replay support — `TimerFired` events in history skip the sleep during replay (deterministic)
- Timer cancellation — if workflow completes before timer fires, timer is cancelled cleanly
- Tests — sleep, timeout-await, workflow timeout, replay after sleep

**Exit criteria:** `Workflow.sleep(Duration.ofSeconds(5))` works end-to-end, survives replay.

---

### Phase 5: Activity Enforcement

**Goal:** Make `ActivityOptions` actually work. Retries, timeouts, and backoff are currently silently ignored.

**Scope:**
- `startToCloseTimeout` enforcement — activity execution killed (virtual thread interrupted) after timeout; appends `ActivityFailed`
- `maxAttempts` with backoff — failed activities retried up to `maxAttempts`; configurable backoff (exponential default, linear option)
- `ActivityOptions.backoffCoefficient` and `initialInterval` — standard retry backoff fields
- Terminal failure — after max retries, `ActivityFailed` written to history with `maxAttemptsExceeded` error type; workflow receives `ActivityFailureException`
- Schedule-to-start timeout — activity task cancelled if not picked up within configured window
- Tests — retry-to-success, retry-to-failure, timeout, backoff timing

**Exit criteria:** Activities respect timeouts and retry policies; workflows receive clean failure after exhausted retries.

---

### Phase 6: Child Workflows

**Goal:** Enable workflows to start other workflows. Foundation for fan-out, saga, and composite workflow patterns.

**Scope:**
- `Workflow.newChildWorkflowStub(Class, ChildWorkflowOptions)` — creates a typed child workflow stub from within parent workflow code
- `ChildWorkflowOptions` — task queue, workflow ID prefix, timeout, retry policy
- Parent-child history linkage — `ChildWorkflowStarted` and `ChildWorkflowCompleted`/`ChildWorkflowFailed` events in parent history
- Child failure propagation — parent receives `ChildWorkflowFailureException` wrapping the child's failure
- Replay support — child workflow result cached in parent's replay state by deterministic child workflow ID
- Async child workflows — fire-and-forget child start (signal-only stub pattern for child)
- Tests — parent-child completion, child failure → parent, async child, replay of parent with completed child

**Exit criteria:** A workflow can start another workflow and use its result; survives crash and replay.

---

### Phase 7: Observability

**Goal:** Make Boudin observable in production. Metrics, tracing, and structured logging.

**Scope:**
- Micrometer integration — optional dependency; `BoudinMetrics` facade wraps Micrometer `MeterRegistry`
- Core metrics:
  - `boudin.workflow.started`, `boudin.workflow.completed`, `boudin.workflow.failed` (counters)
  - `boudin.workflow.duration` (timer histogram by workflow type)
  - `boudin.activity.started`, `boudin.activity.completed`, `boudin.activity.failed` (counters)
  - `boudin.activity.duration` (timer histogram by activity type)
  - `boudin.activity.retries` (counter)
  - `boudin.worker.pending_workflows`, `boudin.worker.pending_activities` (gauges)
- MDC context propagation — `workflowId`, `workflowType`, `activityType` in SLF4J MDC during execution
- Structured log events — key lifecycle events logged at INFO with structured fields
- `Worker.metricsRegistry(MeterRegistry)` — opt-in metrics configuration on worker builder
- Tests — verify metrics incremented on workflow/activity completion and failure

**Exit criteria:** A worker with a Micrometer registry reports workflow and activity metrics; MDC populated during execution.

---

### Phase 8: Document and Version Bump

**Goal:** Document all changes delivered in Milestone 1 and bump the project version to 0.1.0.

**Scope:**
- Update `README.md` — comprehensive developer guide covering all APIs added in Milestone 1: activities (options, retries, timeouts), `Workflow.sleep()`, child workflows, observability (`Worker.metricsRegistry()`, MDC)
- Update `pom.xml` version from `0.1.0-SNAPSHOT` to `0.1.0`
- Add `CHANGELOG.md` — record all features delivered in Milestone 1
- Update any inline code comments or Javadoc that reference "WIP" or "TODO"

**Exit criteria:** README accurately documents the current feature set; version is `0.1.0`; CHANGELOG exists.

---

## Phase Summary

| # | Phase | Key Outcome |
|---|-------|-------------|
| 1 | Foundation & Correctness | No known bugs in existing features |
| 2 | Scalable Crash Recovery | KV checkpoints; startup scans only new events |
| 3 | NIO Event Loop | New execution engine; timer wheel ready |
| 4 | Timers & Workflow.sleep() | `Workflow.sleep()` works end-to-end |
| 5 | Activity Enforcement | Retries and timeouts actually enforced |
| 6 | Child Workflows | Workflows can start workflows |
| 7 | Observability | Metrics and MDC tracing in production |
| 8 | Document & Version Bump | README complete; version `0.1.0`; CHANGELOG |
