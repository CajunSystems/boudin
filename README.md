# Boudin

A Temporal-like durable workflow framework built on the [Gumbo](https://github.com/CajunSystems/gumbo) shared append-only log.

Write ordinary Java methods. Get automatic durability, crash recovery, activity scheduling, and signal-driven coordination — all backed by the log as the single source of truth.

---

## Table of Contents

- [Concepts](#concepts)
- [Architecture](#architecture)
  - [The Log IS the State Store](#the-log-is-the-state-store)
  - [Event Hierarchy](#event-hierarchy)
  - [Threading Model](#threading-model)
  - [Crash Recovery](#crash-recovery)
- [Quick Start](#quick-start)
  - [1. Define a Workflow](#1-define-a-workflow)
  - [2. Define Activities](#2-define-activities)
  - [3. Implement the Workflow](#3-implement-the-workflow)
  - [4. Implement the Activities](#4-implement-the-activities)
  - [5. Wire Up and Run](#5-wire-up-and-run)
  - [6. Send a Signal](#6-send-a-signal)
- [API Reference](#api-reference)
  - [Annotations](#annotations)
  - [Worker](#worker)
  - [WorkflowClient](#workflowclient)
  - [Workflow (static facade)](#workflow-static-facade)
  - [WorkflowOptions / ActivityOptions](#workflowoptions--activityoptions)
  - [Durable Timers](#durable-timers)
  - [Child Workflows](#child-workflows)
  - [Async Start & Workflow Handles](#async-start--workflow-handles)
  - [Queries](#queries)
  - [Observability](#observability)
- [Package Structure](#package-structure)
- [Building](#building)

---

## Concepts

| Term | Meaning |
|------|---------|
| **Workflow** | A durable, long-running computation expressed as a regular Java method. May wait for activities and signals. Automatically replays on restart. |
| **Activity** | A unit of work (I/O, HTTP call, DB write) that runs outside the workflow thread. Results are durably stored so they are never re-executed on replay. |
| **Signal** | An asynchronous message sent to a running workflow that can update its internal state and unblock `Workflow.await()`. |
| **Replay** | On crash recovery the workflow method re-executes from scratch, but `ActivityStub` returns cached results from history instead of scheduling new work. The result is identical to the original run. |
| **Task Queue** | A named channel. Workers poll a queue for workflow tasks and activity tasks. A workflow and its activities can target the same queue or different queues. |
| **Timer** | A durable delay created by `Workflow.sleep(Duration)`. The timer survives crashes — if the process restarts mid-sleep, the timer is re-scheduled for the remaining duration. |
| **Child Workflow** | A workflow started from within another workflow. The parent blocks until the child completes. Results and failures are propagated through the parent's history. |

---

## Architecture

### The Log IS the State Store

There is no separate database. Gumbo's append-only log stores every state transition as a `HistoryEvent` record. The in-memory fields on a workflow implementation class (`private String status`, `private List<Item> cart`) are always 100 % reconstructable from the log alone.

**Log tag conventions:**

| Tag | Contents |
|-----|---------|
| `workflow-tasks:{taskQueue}` | `WorkflowStarted` — one entry per workflow instance |
| `workflow-history:{workflowId}` | Full event history for a single workflow instance |
| `activity-tasks:{taskQueue}` | `ActivityScheduled` — one entry per activity invocation |

When an activity is scheduled, a single atomic append writes the same entry to **both** `workflow-history:{workflowId}` (durable record) and `activity-tasks:{taskQueue}` (delivery to workers). There is no separate two-phase write.

### Event Hierarchy

```
HistoryEvent (sealed interface)
├── WorkflowStarted          (workflowId, workflowType, taskQueue, input: byte[])
├── WorkflowCompleted        (workflowId, result: byte[])
├── WorkflowFailed           (workflowId, errorType, message)
├── ActivityScheduled        (workflowId, activityId, activityType, taskQueue, input, retryPolicy)
├── ActivityCompleted        (workflowId, activityId, result: byte[])
├── ActivityFailed           (workflowId, activityId, errorType, message)
├── SignalReceived           (workflowId, signalName, payload: byte[])
├── TimerStarted             (workflowId, timerId, durationMillis)
├── TimerFired               (workflowId, timerId)
├── ChildWorkflowStarted     (parentWorkflowId, childWorkflowId, childWorkflowType, taskQueue, input)
├── ChildWorkflowCompleted   (parentWorkflowId, childWorkflowId, result: byte[])
└── ChildWorkflowFailed      (parentWorkflowId, childWorkflowId, errorType, message)
```

All events — including domain objects embedded in `input`, `result`, and `payload` fields — are serialized with **Kryo**. Arbitrary POJOs, generics, nested objects, and collections are supported without annotations.

### Threading Model

```
Caller thread                     Worker's dispatcher thread
  stub.process(order)               WorkflowDispatcher.onWorkflowTask()
  ├─ appends WorkflowStarted          ├─ constructs WorkflowRunner
  └─ blocks on resultFuture.join()    └─ starts virtual thread "workflow-{id}"

Virtual thread "workflow-{id}"
  process(order) {
    var v = activities.validate(order)   // parks on CompletableFuture.join()
    // ActivityCompleted arrives → future.complete() → unparks
    Workflow.await(() -> approved)       // parks on signalLock.wait()
    // SignalReceived → field updated → notifyAll() → condition true → unparks
    var r = activities.ship(order)       // parks again
    return r                             // appends WorkflowCompleted → unblocks caller
  }

Virtual thread "activity-{id}"          (one per activity invocation)
  ActivityDispatcher invokes impl.validate(order)
  appends ActivityCompleted to workflow-history
```

Key properties:
- Workflow code runs on **Java virtual threads** — blocking calls are cheap; the carrier thread is never pinned.
- The workflow virtual thread is the only thread that reads or writes workflow fields. No locking is needed on workflow state.
- Activity results and signal deliveries arrive on a Gumbo subscription thread and unblock the workflow thread via `CompletableFuture` / `signalLock.notifyAll()`.

### Crash Recovery

`WorkflowDispatcher` scans `workflow-tasks:{taskQueue}` at startup. For each `WorkflowStarted` whose history does not yet contain `WorkflowCompleted` or `WorkflowFailed`, it:

1. Loads the full `workflow-history:{workflowId}` from the log.
2. Pre-applies all historical `SignalReceived` events to a fresh workflow implementation instance so field values are correct before the virtual thread starts.
3. Builds a `ReplayState` cache of all `ActivityCompleted` and `TimerFired` results.
4. Starts the workflow virtual thread. When `ActivityStub.invoke()` is called, it looks up the cached result and returns it **immediately** — no new log entry is written, the activity is not re-executed.
5. When the virtual thread reaches an activity that has no cached result (the live edge), `ReplayState.finishReplay()` switches to live execution and normal scheduling resumes.

The workflow method re-executes the same code path deterministically, but from the perspective of the workflow, nothing crashed — it simply continued.

---

## Quick Start

### 1. Define a Workflow

```java
@WorkflowInterface
public interface OrderWorkflow {

    @WorkflowMethod
    OrderResult process(Order order);

    @SignalMethod
    void approve(String trackingNumber);
}
```

### 2. Define Activities

```java
@ActivityInterface
public interface OrderActivities {

    @ActivityMethod
    ValidationResult validate(Order order);

    @ActivityMethod
    Shipment ship(Order order, String trackingNumber);
}
```

### 3. Implement the Workflow

```java
public class OrderWorkflowImpl implements OrderWorkflow {

    // Activity stub — created once, reused across invocations
    private final OrderActivities activities =
            Workflow.newActivityStub(OrderActivities.class);

    // Workflow state updated by signals
    private volatile String trackingNumber;

    @Override
    public OrderResult process(Order order) {
        ValidationResult v = activities.validate(order);
        if (!v.isValid()) {
            return OrderResult.rejected(v.reason());
        }

        // Park until approve() signal sets trackingNumber
        Workflow.await(() -> trackingNumber != null);

        Shipment shipment = activities.ship(order, trackingNumber);
        return OrderResult.completed(shipment);
    }

    @Override
    public void approve(String trackingNumber) {
        this.trackingNumber = trackingNumber;  // unblocks Workflow.await()
    }
}
```

### 4. Implement the Activities

```java
public class OrderActivitiesImpl implements OrderActivities {

    @Override
    public ValidationResult validate(Order order) {
        // Any blocking I/O is fine here — runs on a virtual thread
        return inventoryService.check(order);
    }

    @Override
    public Shipment ship(Order order, String trackingNumber) {
        return shippingService.createShipment(order, trackingNumber);
    }
}
```

### 5. Wire Up and Run

```java
SharedLogService sharedLog = SharedLogService.open(
        SharedLogConfig.builder()
                .persistenceAdapter(new FileBasedPersistenceAdapter("/var/boudin"))
                .build());

Worker worker = Worker.newBuilder()
        .taskQueue("orders")
        .sharedLog(sharedLog)
        .build();

worker.registerWorkflow(OrderWorkflowImpl.class);
worker.registerActivities(new OrderActivitiesImpl());
worker.start();

WorkflowClient client = WorkflowClient.newInstance(sharedLog);

OrderWorkflow stub = client.newWorkflowStub(
        OrderWorkflow.class,
        WorkflowOptions.newBuilder()
                .taskQueue("orders")
                .workflowId("order-42")   // optional; random UUID if omitted
                .build());

// Blocks the calling thread until the workflow completes
OrderResult result = stub.process(new Order("ord-42", customer, items));
```

### 6. Send a Signal

Signals can be sent from any thread, including from a different process — only a `SharedLog` reference is needed.

```java
// From the same process
stub.approve("TRK-9871");

// From a different process (by-ID stub — does not start a new workflow)
WorkflowClient client = WorkflowClient.newInstance(sharedLog);
OrderWorkflow signalStub = client.newWorkflowStub(
        OrderWorkflow.class, "order-42", "orders");
signalStub.approve("TRK-9871");
```

---

## API Reference

### Annotations

| Annotation | Target | Purpose |
|-----------|--------|---------|
| `@WorkflowInterface` | Interface | Marks a workflow definition interface |
| `@WorkflowMethod` | Method | The single entry-point method of a workflow |
| `@SignalMethod` | Method | Asynchronous signal handler; must return `void` |
| `@QueryMethod` | Method | Synchronous read of running workflow state; must return a value |
| `@ActivityInterface` | Interface | Marks an activity definition interface |
| `@ActivityMethod` | Method | An activity method callable from workflow code |

`@WorkflowMethod`, `@SignalMethod`, `@QueryMethod`, and `@ActivityMethod` all accept an optional `name` attribute to override the name used on the wire.

### Worker

```java
Worker worker = Worker.newBuilder()
        .taskQueue("my-queue")
        .sharedLog(sharedLog)
        .build();

worker.registerWorkflow(MyWorkflowImpl.class);   // implementation class
worker.registerActivities(new MyActivitiesImpl()); // implementation instance
worker.start();

// Graceful shutdown
worker.close();
```

- `registerWorkflow` accepts the **implementation** class (not the interface). Boudin creates new instances per workflow execution via the no-arg constructor.
- `registerActivities` accepts a pre-constructed **instance** — shared across all activity invocations on this worker.
- Multiple workers can target the same task queue for horizontal scaling.

### WorkflowClient

```java
WorkflowClient client = WorkflowClient.newInstance(sharedLog);

// Start a workflow and block until it completes
MyWorkflow stub = client.newWorkflowStub(MyWorkflow.class, options);
Result r = stub.run(input);

// Start without blocking — returns as soon as the workflow is durably started
WorkflowHandle<Result> handle = client.start(() -> stub.run(input));

// Reattach to any execution by ID, from any process
WorkflowHandle<Result> later = client.getHandle(workflowId, Result.class);

// Target an already-running workflow by ID (no new execution started).
// Supports @SignalMethod and @QueryMethod; calling @WorkflowMethod throws.
MyWorkflow existing = client.newWorkflowStub(MyWorkflow.class, workflowId, taskQueue);
existing.mySignal(payload);
String state = existing.myQuery();
```

`WorkflowClient` does not require a running `Worker` — it only writes to and reads from the shared log.

### Workflow (static facade)

All methods below may only be called from within a running workflow method (on the workflow virtual thread).

```java
// Create a typed activity proxy (safe to store as a field)
MyActivities activities = Workflow.newActivityStub(MyActivities.class);
MyActivities activities = Workflow.newActivityStub(MyActivities.class, ActivityOptions.defaults());

// Durable sleep — appends TimerStarted; parks virtual thread; resumes after duration
// Survives crash: if the process restarts mid-sleep, the remaining duration is re-scheduled
Workflow.sleep(Duration.ofMinutes(5));

// Create a typed child workflow stub (safe to store as a field or call inline)
ChildWorkflow child = Workflow.newChildWorkflowStub(ChildWorkflow.class);
ChildWorkflow child = Workflow.newChildWorkflowStub(ChildWorkflow.class,
        ChildWorkflowOptions.newBuilder().taskQueue("other-queue").build());

// Block until condition is true (re-evaluated after each signal)
Workflow.await(() -> this.approved);

// Block with a timeout; returns false if timeout elapsed before condition became true
boolean inTime = Workflow.await(Duration.ofMinutes(10), () -> this.approved);

// Identity
String id    = Workflow.getWorkflowId();
String queue = Workflow.getTaskQueue();
```

### WorkflowOptions / ActivityOptions

```java
WorkflowOptions options = WorkflowOptions.newBuilder()
        .taskQueue("orders")           // required
        .workflowId("order-42")        // optional; random UUID if omitted
        .workflowRunTimeout(Duration.ofHours(24))  // optional
        .build();

ActivityOptions actOpts = ActivityOptions.newBuilder()
        .taskQueue("heavy-tasks")               // optional; defaults to workflow's queue
        .startToCloseTimeout(Duration.ofSeconds(30))  // default: 10s per attempt
        .scheduleToStartTimeout(Duration.ofSeconds(60)) // default: none; max wait before execution begins
        .maxAttempts(3)                         // default: 1
        .initialInterval(Duration.ofSeconds(2)) // default: 1s; delay before first retry
        .backoffCoefficient(1.5)                // default: 2.0; multiplier per subsequent retry
        .build();
```

Retry delay follows exponential backoff: `delay(n) = initialInterval × backoffCoefficient^(n-1)`.
With the defaults (1s interval, coefficient 2.0): attempt 2 waits 1s, attempt 3 waits 2s, attempt 4 waits 4s.
Set `backoffCoefficient(1.0)` for a constant retry interval.

### Durable Timers

`Workflow.sleep(Duration)` appends a `TimerStarted` event to the workflow's history, schedules the
timer through the `HashedWheelTimer`, and parks the workflow virtual thread. When the timer fires,
`TimerFired` is appended and the virtual thread resumes.

```java
public class ReminderWorkflowImpl implements ReminderWorkflow {
    @Override
    public void run(String userId) {
        // Send an immediate notification
        activities.notify(userId, "Your trial started!");

        // Wait 7 days durably (survives process restart)
        Workflow.sleep(Duration.ofDays(7));

        // Send a follow-up notification
        activities.notify(userId, "Your trial ends tomorrow!");
    }
}
```

On crash recovery, if the timer has not yet fired, Boudin calculates the remaining duration
from the `TimerStarted` timestamp and re-schedules it. If the timer already fired before the
crash, `TimerFired` is in the history and the sleep returns immediately during replay.

### Child Workflows

A workflow can start another workflow and block until it completes using
`Workflow.newChildWorkflowStub()`. Child workflows run independently — they have their own
workflow ID, history, and task queue.

```java
@WorkflowInterface
public interface ValidationWorkflow {
    @WorkflowMethod
    ValidationResult validate(Order order);
}

public class OrderWorkflowImpl implements OrderWorkflow {

    // Child workflow stub — inherits parent's task queue by default
    private final ValidationWorkflow validator =
            Workflow.newChildWorkflowStub(ValidationWorkflow.class);

    // To target a different task queue:
    // Workflow.newChildWorkflowStub(ValidationWorkflow.class,
    //     ChildWorkflowOptions.newBuilder().taskQueue("validators").build());

    @Override
    public OrderResult process(Order order) {
        ValidationResult v = validator.validate(order);  // blocks until child completes
        if (!v.isValid()) return OrderResult.rejected(v.reason());
        // ...
    }
}
```

If the child workflow fails, `ChildWorkflowFailureException` is thrown:

```java
try {
    ValidationResult v = validator.validate(order);
} catch (ChildWorkflowFailureException e) {
    log.warn("Validation failed: {} — {}", e.errorType(), e.getMessage());
    return OrderResult.rejected("validation-error");
}
```

The child workflow must be registered on a worker that listens to the target task queue.
Both parent and child implementations can be registered on the same `Worker` instance.

### Async Start & Workflow Handles

Calling a `@WorkflowMethod` on a stub blocks for the workflow's entire lifetime. That is wrong
for anything request-scoped: a workflow can run for days, and no HTTP handler can wait. Start it
without blocking and collect the result whenever — or wherever — you like.

```java
OrderWorkflow stub = client.newWorkflowStub(OrderWorkflow.class, options);

// Returns as soon as the workflow is durably recorded in the log
WorkflowHandle<OrderResult> handle = client.start(() -> stub.process(order));

return handle.workflowId();   // hand this to the caller and return
```

The lambda calls the workflow method as normal Java, so the compiler type-checks the arguments —
but inside `start(...)` the stub only records the start and returns a placeholder, so **ignore
the lambda's own return value**. Use `client.start(() -> stub.someVoidMethod(x))` for a `void`
workflow method; the `Runnable` overload is selected automatically.

Later, in the same process or a different one, the workflow ID is all you need:

```java
WorkflowHandle<OrderResult> handle = client.getHandle(workflowId, OrderResult.class);

OrderResult r = handle.getResult();                       // block until done
OrderResult r = handle.getResult(Duration.ofSeconds(30)); // or give up waiting
CompletableFuture<OrderResult> f = handle.getResultAsync();
```

`getResult` works **after** the workflow has finished, however long ago — it reads the terminal
event from history rather than waiting for one to arrive. A failed workflow throws
`WorkflowFailureException`, the same contract as a blocking stub call. `getResult(Duration)`
throws `WorkflowTimeoutException`, which describes *your wait* expiring, not the workflow: it is
still running, and the same handle can be waited on again.

#### Inspecting an execution

```java
WorkflowExecutionDescription d = handle.describe();

d.status();        // RUNNING | COMPLETED | FAILED
d.workflowType();  // "OrderWorkflow"
d.taskQueue();
d.startedAt();
d.closedAt();      // null while RUNNING
d.historyLength();
d.failure();       // errorType + message, null unless FAILED
```

`describe()` needs no running worker — it is derived from the log, so it answers for completed
and unhosted executions. That is the difference from a query: a `@QueryMethod` reports live
in-memory state and only while a worker hosts the workflow.

#### Listing what is running

```java
List<String> ids = client.listWorkflows("orders");
```

This reads the dispatcher's active-workflow set for one task queue. Read it for what it is
rather than as a query index:

- it covers a single task queue, not the whole log
- a workflow whose worker died without processing its own terminal event stays listed until a
  worker recovers it
- it is empty until a worker has run on that queue, even if workflows were started

Use `describe()` for the authoritative state of any single execution.

---

### Queries

A `@QueryMethod` reads the current state of a **running** workflow without waiting for it to
finish and without disturbing it. Queries are synchronous and return a value.

```java
@WorkflowInterface
public interface OrderWorkflow {

    @WorkflowMethod
    OrderResult process(Order order);

    @SignalMethod
    void approve(String trackingNumber);

    @QueryMethod
    String getStatus();

    @QueryMethod
    int getItemCount();
}
```

Implement them as ordinary getters over workflow fields:

```java
public class OrderWorkflowImpl implements OrderWorkflow {

    private volatile String status = "PENDING";
    private final List<Item> items = new ArrayList<>();

    @Override public String getStatus()  { return status; }
    @Override public int getItemCount()  { return items.size(); }
    // ...
}
```

Call them on any stub — including one built from just a workflow ID in another process:

```java
// On the stub that started the workflow
String status = stub.getStatus();

// From another process — only a SharedLog reference is needed
WorkflowClient client = WorkflowClient.newInstance(sharedLog);
OrderWorkflow observer = client.newWorkflowStub(OrderWorkflow.class, "order-42", "orders");
String status = observer.getStatus();
```

#### How queries travel

Queries are **control plane, not history**. A `QueryRequested` message goes to a dedicated
`workflow-queries:{workflowId}` tag; the worker running the workflow answers on the same tag
with `QueryCompleted` or `QueryFailed`, correlated by a per-request `queryId`.

| Property | Behaviour |
|----------|-----------|
| Workflow history | Untouched — query traffic never enters `workflow-history:{workflowId}` |
| Replay | Unaffected; queries are not replayed and do not grow history |
| Concurrency | Handlers run under the workflow's signal lock, so a query never interleaves with a signal handler |
| Visibility | A query may observe the workflow thread between yield points |
| Timeout | `WorkflowOptions.queryTimeout` — default 10s |

#### Rules and failure modes

- A query method must return a value. `void` is rejected at `registerWorkflow` time.
- Query names must be unique within an interface; use `@QueryMethod(name = "...")` to disambiguate.
- Handlers **must not** modify workflow state. This is not enforced — a mutating handler will
  corrupt deterministic replay.
- Handlers **must not** block. They hold the workflow's signal lock while running, so this
  workflow's signal deliveries and other queries queue behind a slow handler. Other workflows
  on the worker are unaffected — signal delivery runs on a per-workflow thread so one slow
  handler cannot stall the shared event loop.

Everything that can go wrong surfaces as `WorkflowQueryException`, distinguished by `errorType()`:

| `errorType()` | Cause |
|---------------|-------|
| `UnknownQuery` | No `@QueryMethod` with that name on the registered workflow interface |
| `QueryTimedOut` | Nothing answered in time — the workflow already completed, or no worker is running it |
| *exception simple name* | The query handler threw |

```java
try {
    String status = observer.getStatus();
} catch (WorkflowQueryException e) {
    if ("QueryTimedOut".equals(e.errorType())) {
        // workflow is finished or unhosted — fall back to durable state
    }
}
```

> Queries are answered only while a worker is actively running the workflow. Reading the
> terminal state of a **completed** workflow is not a query — that is `WorkflowHandle.describe()`,
> arriving in 0.2.0.

---

### Observability

#### Metrics

Boudin integrates with [Micrometer](https://micrometer.io/). Pass a `MeterRegistry` to the
worker builder to enable metrics:

```java
MeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

Worker worker = Worker.newBuilder()
        .taskQueue("orders")
        .sharedLog(sharedLog)
        .metricsRegistry(registry)   // opt-in; omit to disable metrics
        .build();
```

Metrics recorded automatically:

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `boudin.workflow.started` | Counter | `workflowType` | Incremented when a workflow begins executing |
| `boudin.workflow.completed` | Counter | `workflowType` | Incremented on successful completion |
| `boudin.workflow.failed` | Counter | `workflowType` | Incremented when workflow throws |
| `boudin.workflow.duration` | Timer | `workflowType` | Wall-clock execution time |
| `boudin.activity.started` | Counter | `activityType` | Incremented per scheduling event |
| `boudin.activity.completed` | Counter | `activityType` | Incremented on successful execution |
| `boudin.activity.failed` | Counter | `activityType` | Incremented when all retries are exhausted |
| `boudin.activity.retries` | Counter | `activityType` | Incremented for each retry attempt |
| `boudin.activity.duration` | Timer | `activityType` | Total time from scheduling to completion/failure |
| `boudin.worker.pending_workflows` | Gauge | `taskQueue` | Currently executing workflow instances |
| `boudin.worker.pending_activities` | Gauge | `taskQueue` | Currently executing activity instances |

`micrometer-core` is declared as an optional Maven dependency — add it explicitly if you want
to use a specific registry (Prometheus, Datadog, InfluxDB, etc.).

#### MDC Context

Boudin sets SLF4J MDC keys automatically during execution so that all log statements within a
workflow or activity carry context:

| Key | Value | Set during |
|-----|-------|-----------|
| `workflowId` | The workflow instance ID | Workflow virtual thread |
| `workflowType` | The `@WorkflowInterface` simple name | Workflow virtual thread |
| `activityId` | The unique activity invocation ID | Activity execution |
| `activityType` | `"InterfaceName#methodName"` | Activity execution |

These keys are cleared automatically when the workflow/activity finishes.

---

## Package Structure

```
com.cajunsystems.boudin
├── annotation/         @WorkflowInterface, @WorkflowMethod, @SignalMethod,
│                       @QueryMethod, @ActivityInterface, @ActivityMethod
├── api/                Worker, WorkflowClient, WorkflowStub, SignalOnlyStub, QueryClient,
│                       WorkflowHandle, WorkflowExecutionDescription, WorkflowStatus,
│                       WorkflowOptions, WorkflowFailureException, WorkflowQueryException,
│                       WorkflowTimeoutException
├── activity/           ActivityStub (InvocationHandler), ActivityOptions,
│                       ActivityFailureException
├── history/            HistoryEvent (sealed hierarchy, 13 record types), HistorySerializer
├── query/              QueryMessage (QueryRequested/Completed/Failed), QuerySerializer
├── internal/           WorkflowDispatcher, ActivityDispatcher, WorkflowRunner,
│                       WorkflowRegistry, ActivityRegistry, ReplayState,
│                       BoudinEventLoop, HashedWheelTimer, BoudinMetrics
├── serialization/      KryoSerializer (thread-safe Kryo pool)
└── workflow/           Workflow (static facade), WorkflowContext, WorkflowThread,
                        ChildWorkflowOptions, ChildWorkflowFailureException
```

**Data flow summary:**

```
WorkflowClient
  └─ appends WorkflowStarted ──────────────────────────────────────────┐
                                                                        ▼
                                                             workflow-tasks:{queue}
                                                                        │
                                                             WorkflowDispatcher
                                                                        │ creates
                                                             WorkflowRunner
                                                                        │ starts VT
                                                             workflow VT runs
                                                                        │ calls ActivityStub
ActivityStub appends ActivityScheduled ─────────────────────────────────┤
  (atomic dual-tag write)                                               ▼
                                            activity-tasks:{queue}   workflow-history:{id}
                                                     │
                                          ActivityDispatcher
                                                     │ executes impl
                                                     │ appends ActivityCompleted
                                                     ▼
                                             workflow-history:{id}
                                                     │
                                          WorkflowRunner delivers
                                                     │ to WorkflowContext
                                                     ▼
                                          workflow VT unparks, continues
                                                     │ returns value
                                          WorkflowThread appends WorkflowCompleted
                                                     │
                                          WorkflowClient.startAndWait() unblocks
```

---

## Building

Boudin requires **Java 21** and pulls Gumbo via [JitPack](https://jitpack.io).

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.cajunsystems</groupId>
    <artifactId>boudin</artifactId>
    <version>0.1.0</version>
</dependency>
```

Run tests (uses an in-memory Gumbo persistence adapter — no disk I/O required):

```bash
mvn verify
```
