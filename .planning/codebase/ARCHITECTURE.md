# Architecture

## What This Is

Boudin is a **durable workflow orchestration framework** modeled after Temporal/Cadence. It implements **event sourcing** with **deterministic replay** for fault tolerance.

Core principle: *The log is the state store.* All state transitions are persisted as immutable events in an append-only log (Gumbo). In-memory state is 100% reconstructable from the log.

## Key Abstractions

| Abstraction | Responsibility |
|-------------|---------------|
| `WorkflowClient` | Client entry point; creates typed workflow stubs via dynamic proxies |
| `WorkflowStub` | Intercepts workflow/signal calls; writes `WorkflowStarted`/`SignalReceived` to log |
| `Worker` | Server-side entry point; manages `WorkflowDispatcher` + `ActivityDispatcher` lifecycle |
| `WorkflowDispatcher` | Subscribes to `workflow-tasks:{queue}`; creates `WorkflowRunner` per execution |
| `WorkflowRunner` | Per-workflow lifecycle; builds replay state, subscribes to history, routes events |
| `WorkflowThread` | Runs workflow method on a virtual thread; appends `WorkflowCompleted`/`Failed` |
| `WorkflowContext` | Per-instance state: pending activity futures, signal lock, replay state |
| `ActivityStub` | Proxy for activity calls in workflow code; handles replay vs. live scheduling |
| `ActivityDispatcher` | Subscribes to `activity-tasks:{queue}`; executes activities on virtual threads |
| `ReplayState` | Pre-loads completed activity results from history; generates deterministic IDs |
| `HistoryEvent` | Sealed interface with 9 record event types (the event model) |

## Log Tag Structure

```
workflow-tasks:{taskQueue}      — new workflow start events (task queue for discovery)
workflow-history:{workflowId}   — complete event history per workflow instance
activity-tasks:{taskQueue}      — activity invocation events (activity task queue)
```

## Core Execution Flow

```
CLIENT:
  workflow.processOrder(order)
    → WorkflowStub: pre-subscribe to workflow-history:{id}
    → Atomic append: WorkflowStarted → workflow-tasks:{q} + workflow-history:{id}
    → Block on CompletableFuture

WORKER (WorkflowDispatcher):
  receives WorkflowStarted from workflow-tasks:{q}
    → creates WorkflowRunner

WorkflowRunner.start():
  → load history, build ReplayState (cache completed activities)
  → replay historical signals onto workflow impl
  → subscribe to workflow-history:{id} (tail only)
  → start WorkflowThread (virtual thread)

WorkflowThread:
  → invoke workflowMethod(args) via reflection
  → workflow calls activities via ActivityStub proxy:
      REPLAY:  return cached result immediately (no log write)
      LIVE:    atomic append: ActivityScheduled → workflow-history:{id} + activity-tasks:{q}
               park on future.join()
  → append WorkflowCompleted/Failed
  → complete client's CompletableFuture

ActivityDispatcher:
  receives ActivityScheduled from activity-tasks:{q}
    → spawn virtual thread
    → execute activity impl
    → append ActivityCompleted/Failed to workflow-history:{id}

WorkflowRunner (history listener):
  receives ActivityCompleted
    → context.deliverActivityResult() → completes future → workflow thread unparks
```

## Deterministic Replay

The replay mechanism is how crash recovery works:

1. **Pre-load cache**: `ReplayState` scans history, caches `ActivityCompleted` results by deterministic ID
2. **Deterministic IDs**: `{workflowId}:{ActivityType#method}:{sequenceNum}` — same history → same IDs
3. **Replay mode**: `ActivityStub` checks cache; if hit → return bytes immediately, no log write
4. **Live edge**: First activity not in cache → `finishReplay()` → switch to live execution
5. **Signal replay**: Historical signals applied to workflow impl *before* virtual thread starts

## Signal Handling

```
client.cancelOrder(reason)
  → WorkflowStub: append SignalReceived → workflow-history:{id}
  → returns immediately (async)

WorkflowRunner (history listener):
  → invoke @SignalMethod on impl (mutates state, e.g. this.cancelled = true)
  → signalLock.notifyAll() → wakes any Workflow.await() threads

workflow.await(() -> this.cancelled):
  → synchronized on signalLock
  → re-evaluates BooleanSupplier after each signal
```

## Threading Model

- Every workflow instance: one virtual thread (`Thread.ofVirtual()`)
- Every activity execution: one virtual thread
- `WorkflowContext` stored in `ThreadLocal<WorkflowContext>` — enables `Workflow.*` static API
- Activity futures: `ConcurrentHashMap<activityId, CompletableFuture<byte[]>>`
- Signal waiting: `synchronized (signalLock) { signalLock.wait() }`

## What Is NOT Yet Implemented

- **Timers/sleep**: Infrastructure exists (`TimerStarted`/`TimerFired` events, replay tracking) but no `Workflow.sleep()` API and no timer-firing service
- **Query methods**: Annotation defined, registry method exists, but stubs throw `UnsupportedOperationException`
- **Activity timeouts**: `ActivityOptions.startToCloseTimeout` defined but never enforced
- **Activity retries**: `ActivityOptions.maxAttempts` defined but never consulted
- **Workflow timeout**: `WorkflowOptions.workflowRunTimeout` defined but never enforced
