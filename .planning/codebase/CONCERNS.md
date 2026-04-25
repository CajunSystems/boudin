# Concerns

## Critical

### Timer/Sleep API Missing
Infrastructure exists (`TimerStarted`/`TimerFired` events, `pendingTimers` in `WorkflowContext`, `timerSequences` in `ReplayState`) but there is no `Workflow.sleep()` public API and no timer-firing service. Workflows cannot delay or schedule future work.

### Unbounded Startup Log Scan
`WorkflowDispatcher.start()` and `ActivityDispatcher.start()` call `.readAll().join()` to scan the entire log on every worker restart. In production with millions of historical events, this causes severe memory pressure and slow startup. No pagination, streaming, or checkpoint-based recovery.

### Query Methods Unimplemented
`@QueryMethod` annotation and `WorkflowRegistry.findQueryMethod()` exist, but both `WorkflowStub` and `SignalOnlyStub` throw `UnsupportedOperationException`. No way to read workflow state without waiting for completion.

### Activity Timeouts/Retries Not Enforced
`ActivityOptions` has `startToCloseTimeout` and `maxAttempts` fields that are stored but never consulted. A hung activity blocks the workflow forever. A failed activity is not retried.

### Workflow Timeout Not Enforced
`WorkflowOptions.workflowRunTimeout` is defined but never checked. Workflows can run indefinitely.

## High

### No Exactly-Once Activity Semantics
If a worker crashes after activity execution but before appending `ActivityCompleted`, the activity re-executes on recovery. Activities with side effects (payment charges, email sends) can execute more than once. The code comment says "at most once per session" — this is per-JVM, not per-cluster.

### Subscription Callbacks Can Block Delivery
`ActivityDispatcher` callback calls `isAlreadyCompleted()` which does a blocking `readAll().join()`. If this hangs, event delivery to all workflows/activities on that worker is stalled.

### Startup Scan Doesn't Scale
Same as critical item #2 above — full log scan creates memory risk proportional to total history size, with no upper bound.

### Signal Delivery Not Idempotent
If a `SignalReceived` event is delivered twice (subscription replay or glitch), the signal method is invoked twice. No deduplication. Signal handlers with side effects (incrementing counters, etc.) will fire twice.

### Worker Shutdown Doesn't Guarantee Thread Stop
`WorkflowRunner.close()` interrupts the virtual thread but does not wait for it to stop or verify it stopped. Virtual thread interrupt semantics are different from platform threads — the workflow may continue executing briefly after shutdown.

### Pending Activities Map Never Pruned
`WorkflowContext.pendingActivities` (`ConcurrentHashMap`) grows as activities are scheduled and entries are never removed after completion. Long-running workflows with high activity throughput accumulate garbage.

### No Validation at Workflow/Activity Registration
`WorkflowRegistry` and `ActivityRegistry` don't validate that:
- Exactly one `@WorkflowMethod` exists per workflow interface
- No duplicate method names across registered activity interfaces
- All required annotations are present
Errors only surface at runtime when the workflow is invoked.

## Medium

### Activity ID Generation Collision Risk
Activity IDs are `{workflowId}:{activityType#method}:{sequenceNum}`. The sequence counter is per-activity-type. If workflow code uses conditional branching that changes which activities are called on replay vs. live, IDs diverge and the wrong cached result is returned. No determinism enforcement.

### Hardcoded Log Tag Names
Log tag naming (`workflow-history`, `workflow-tasks`, `activity-tasks`) is repeated as string literals across multiple files. No central constant. Typos or naming changes require multi-file updates.

### No Schema Evolution for History Events
Records don't carry version fields. If the fields of any `HistoryEvent` record change, old history cannot be replayed. No migration path.

### Missing Observability
No metrics, no distributed tracing, minimal logging. No counters for workflow/activity latency, failure rates, replay ratios, or pending activity queue depth. Production debugging will be hard.

### Signal Replay Exception Swallowed
In `WorkflowRunner.replayHistoricalSignals()`, if a signal handler throws, the exception is logged and swallowed. The workflow starts in a potentially inconsistent state.

### No Parent-Child Workflows
No mechanism to start a workflow from within another workflow. Each workflow is independent.

## Low

### No Configurable Limits
Kryo pool size (16), subscription polling behavior, activity concurrency are all hardcoded. No configuration surface for tuning.

### Awaitility Imported But Unused in Tests
`Awaitility` is a test dependency but not used in `GreetingWorkflowTest`. Async tests use `Thread.sleep(200)` instead, which is fragile and timing-dependent.

### No Compression of History
Large workflow histories (many activities, large payloads) are stored and transmitted uncompressed.

### No Completed Workflow Cleanup
The log grows indefinitely. There is no TTL, archival, or cleanup mechanism for completed workflows.

### Missing Test Coverage for Crash Recovery
The test suite only covers happy-path execution. Crash recovery, activity failure, signal ordering, and timeout scenarios are untested.

## Feature Gap vs. Temporal/Cadence

| Feature | Status |
|---------|--------|
| Workflow execution + durability | ✅ Implemented |
| Activity execution + basic error handling | ✅ Implemented |
| Signal support | ✅ Implemented |
| Query support | ❌ Stub only (throws) |
| Activity timeouts | ❌ Option exists, not enforced |
| Activity retries | ❌ Option exists, not used |
| Workflow sleep/timers | ❌ Infrastructure only, no public API |
| Workflow timeout | ❌ Option exists, not enforced |
| Exactly-once activity semantics | ❌ At-least-once |
| Parent-child workflows | ❌ Not present |
| Cron workflows | ❌ Not present |
| Saga/compensation patterns | ❌ Not present |
