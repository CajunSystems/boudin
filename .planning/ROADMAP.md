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

---
---

## Milestone 2: Operable Boudin

Milestone 1 produced a correct engine. Milestone 2 makes it something you can put behind an
HTTP API and operate: read the state of a running workflow, start work without blocking a
caller thread, stop work that should not continue, scale workers horizontally without
double-executing, and run workflows that never end.

**Theme:** the control plane. Every phase adds a way to observe or steer an execution from
outside it.

**Version target:** `0.2.0`

---

### Phase 8.1: Gumbo 0.6.0 Upgrade (inserted)

**Goal:** Unblock full-suite verification. `mvn verify` hung indefinitely on `main` — traced to
gumbo 0.2.0 dropping any log entry appended while a subscription was still delivering its
backlog, leaving Boudin callers blocked forever on a terminal event that was never delivered.

**Delivered:**
- `pom.xml` gumbo `0.2.0` → `0.6.0`; no Boudin code changes required
- `SubscriptionCatchUpTest` — regression guard that fails on 0.2.0 and passes on 0.6.0
- Also picks up 0.6.0's multi-tag stream-numbering fix, which silently affected Boudin's
  dual-tag history + task-queue append, and 0.4.0's conditional KV that Phase 12's leases need

**Exit criteria:** `mvn verify` completes green. ✅ 37/37, three consecutive runs.

---

### Phase 9: Query Methods

**Goal:** Read the state of a running workflow without waiting for it to finish. `@QueryMethod`
exists as an annotation and a registry lookup but both stubs throw `UnsupportedOperationException`
— today there is no way to observe an in-flight execution at all.

**Scope:**
- `QueryMessage` sealed hierarchy (`QueryRequested`, `QueryCompleted`, `QueryFailed`) in a new
  `query` package with its own Kryo serializer — deliberately **not** part of `HistoryEvent`
- New log tag `workflow-queries:{workflowId}` — queries are control-plane traffic and must never
  enter `workflow-history`, which is replayed
- `WorkflowRunner` subscribes to the query tag; resolves the handler via
  `WorkflowRegistry.findQueryMethod()`; invokes it on the workflow impl under `signalLock` so a
  query never interleaves with a signal handler
- Query round-trip on both `WorkflowStub` and `SignalOnlyStub` (cross-process queries work with
  only a `SharedLog` reference)
- `WorkflowQueryException` — clear failure for unknown query name, handler throw, or no worker
  currently running the workflow
- `WorkflowOptions.queryTimeout` (default 10s) — client-side bound on the round trip
- Registration validation — reject query methods that return `void`
- Fix `README` reference to the non-existent `client.newSignalOnlyStub(...)`

**Exit criteria:** `stub.getStatus()` on a running workflow returns live state from another
thread and another process; unknown queries and handler failures surface as
`WorkflowQueryException`.

---

### Phase 10: Async Start & Workflow Handles

**Goal:** Stop forcing the caller to block. `stub.process(order)` currently blocks the calling
thread for the entire workflow duration, and nothing can reattach to a running execution to
collect its result.

**Scope:**
- `WorkflowHandle<T>` — `workflowId()`, `getResult()`, `getResult(Duration)`,
  `getResultAsync()`, `describe()`
- `WorkflowClient.start(stub, args)` — starts and returns a handle without blocking
- `WorkflowClient.getHandle(workflowInterface, workflowId)` — reattach to an execution started
  by another process
- Result retrieval for **already-completed** workflows — read the terminal event from history
  instead of subscribing forever
- `WorkflowExecutionDescription` — status (`RUNNING`/`COMPLETED`/`FAILED`), type, task queue,
  start time, close time
- `WorkflowClient.listWorkflows(taskQueue)` — read the active-workflow KV set

**Exit criteria:** A web request handler can start a workflow, return immediately, and a later
request in a different process can fetch the result by workflow ID.

---

### Phase 11: Cancellation & Termination

**Goal:** Make it possible to stop a workflow. There is currently no cancel API — a workflow
parked in `Workflow.await()` waits forever.

**Scope:**
- `WorkflowCancelRequested` history event; `WorkflowCancelled` terminal event
- `WorkflowHandle.cancel(reason)` — cooperative: sets the cancellation flag, wakes `signalLock`
- `Workflow.isCancelRequested()` and `Workflow.cancellationScope(...)` — workflow code observes
  and handles cancellation
- Cancellation-aware `Workflow.await()` and `Workflow.sleep()` — throw `CancelledException`
  rather than parking forever
- `WorkflowHandle.terminate(reason)` — hard stop: appends `WorkflowFailed`, interrupts the
  virtual thread, removes from the active set, no cooperation required
- Cancellation propagates to running child workflows
- `WorkflowOptions.workflowRunTimeout` enforcement rides on the same machinery (currently
  defined but never checked)

**Exit criteria:** A parked workflow can be cancelled and observe it; `terminate` stops a
misbehaving workflow unconditionally.

---

### Phase 12: Worker Task Claiming

**Goal:** Make horizontal scale-out correct. The README already promises "multiple workers can
target the same task queue", but there is no claim step — every subscribed worker sees every
`ActivityScheduled` and the only guard is a racy `isAlreadyCompleted()` history read. Two
workers on one queue will both execute the same activity.

**Scope:**
- KV-based claim/lease per activity ID and per workflow ID, using gumbo 0.4.0's
  `compareAndSetTagValue` (available since the Phase 8.1 upgrade)
- Refuse to start distributed execution unless the configured adapter declares
  `conditionalAppend` + `multiWriter` via gumbo 0.5.0's `LogCapabilities`, rather than assuming
  it — note `BatchingPersistenceAdapter` forces `multiWriter` off however capable its delegate is
- Lease record: owner worker ID, expiry timestamp; only the claim winner executes
- Lease renewal while an activity runs; expiry releases the task for another worker
- Orphan recovery — a task whose lease expired without a terminal event is re-claimable
- Per-worker identity (`Worker.workerId`, defaulted to host + UUID)
- **Fix:** `ActivityDispatcher` checkpoint KV key is per-tag, so two workers on one queue
  currently clobber each other's checkpoint — key it by worker ID
- Tests — two workers, one queue: each activity executes exactly once; killed worker's lease
  expires and the survivor picks the task up

**Exit criteria:** Two workers on the same task queue execute each activity exactly once, and a
killed worker's in-flight tasks are recovered by the survivor.

---

### Phase 13: Continue-As-New

**Goal:** Support workflows that never end. Any long-lived loop (subscription billing, polling
with `Workflow.sleep`) grows its history tag without bound and replays the whole thing on
recovery.

**Scope:**
- `Workflow.continueAsNew(args)` — closes the current run and starts a fresh execution with a
  new history
- `WorkflowContinuedAsNew` terminal history event carrying the next run ID
- Run chaining — `{workflowId}:run:{n}`; a handle for the logical workflow ID follows the chain
  to the currently-active run
- `WorkflowHandle.getResult()` returns the result of the final run in the chain
- `WorkflowOptions.historySizeLimit` — a warning metric when a history exceeds a threshold, so
  users learn they need continue-as-new before they hit trouble

**Exit criteria:** A workflow can loop indefinitely with bounded history per run; a client
handle resolves the current run transparently.

---

### Phase 14: Document and Version Bump

**Goal:** Document Milestone 2 and release `0.2.0`.

**Scope:**
- README sections for queries, async start / handles, cancellation, worker scale-out, continue-as-new
- Update the "Feature Gap vs. Temporal/Cadence" table in `.planning/codebase/CONCERNS.md`
- `CHANGELOG.md` entry for `0.2.0`
- `pom.xml` version bump to `0.2.0`

**Exit criteria:** README documents the current feature set; version is `0.2.0`; CHANGELOG updated.

---

## Milestone 2 Phase Summary

| # | Phase | Key Outcome |
|---|-------|-------------|
| 9 | Query Methods | Read live workflow state without waiting for completion |
| 10 | Async Start & Handles | Non-blocking start; reattach to a run from any process |
| 11 | Cancellation & Termination | Stop a workflow cooperatively or forcibly |
| 12 | Worker Task Claiming | Multi-worker scale-out executes each task exactly once |
| 13 | Continue-As-New | Unbounded-duration workflows with bounded history |
| 14 | Document & Version Bump | README complete; version `0.2.0`; CHANGELOG |

---

## Deferred to Milestone 3+

Considered for Milestone 2 and consciously deferred — recorded so the reasoning is not lost:

| Candidate | Why deferred |
|-----------|--------------|
| Determinism toolkit (`Workflow.sideEffect`, `currentTimeMillis`, `randomUUID`) + replay-divergence detection | High value, but pairs naturally with versioning; both belong in one "safe to deploy twice" milestone |
| Workflow versioning / `Workflow.getVersion()` patching | Same — the deploy-safety milestone |
| Async activities (`Workflow.async` → `Promise`, `allOf`/`anyOf`) | Large surface; parallel fan-out is a performance feature, not a correctness one |
| Activity heartbeats + `heartbeatTimeout` | Depends on Phase 12 leases landing first |
| Saga / compensation helper | Pure library layer on top; no engine changes needed, so it can land any time |
| Idempotency keys for activities | Needs Phase 12 claiming to be meaningful |
| Cron / scheduled workflows | Straightforward on the existing timer wheel; not blocking anything |
| History retention & archival | Continue-as-new (Phase 13) relieves most of the pressure |
| Interceptor chain (tracing, auth propagation) | Wants a stable public API surface; better after Milestone 2 settles it |
| Spring Boot starter | Adoption lever, not an engine feature; separate module |
| `boudin` CLI (describe/history/signal/list) | Best built on Phase 10's `describe()`/`listWorkflows()` |
| `HistoryEvent` schema versioning | Should land before the first release that promises history compatibility |
