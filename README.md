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
├── WorkflowStarted     (workflowId, workflowType, taskQueue, input: byte[])
├── WorkflowCompleted   (workflowId, result: byte[])
├── WorkflowFailed      (workflowId, errorType, message)
├── ActivityScheduled   (workflowId, activityId, activityType, taskQueue, input: byte[])
├── ActivityCompleted   (workflowId, activityId, result: byte[])
├── ActivityFailed      (workflowId, activityId, errorType, message)
├── SignalReceived      (workflowId, signalName, payload: byte[])
├── TimerStarted        (workflowId, timerId, durationMillis)
└── TimerFired          (workflowId, timerId)
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

// From a different process (signal-only stub — does not start a new workflow)
WorkflowClient client = WorkflowClient.newInstance(sharedLog);
OrderWorkflow signalStub = client.newSignalOnlyStub(
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
| `@QueryMethod` | Method | (Reserved — not yet implemented) |
| `@ActivityInterface` | Interface | Marks an activity definition interface |
| `@ActivityMethod` | Method | An activity method callable from workflow code |

`@WorkflowMethod`, `@SignalMethod`, and `@ActivityMethod` all accept an optional `name` attribute to override the name stored in the event history.

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

// Send a signal to an already-running workflow (no new execution started)
MyWorkflow signalStub = client.newSignalOnlyStub(MyWorkflow.class, workflowId, taskQueue);
signalStub.mySignal(payload);
```

`WorkflowClient` does not require a running `Worker` — it only writes to and reads from the shared log.

### Workflow (static facade)

All methods below may only be called from within a running workflow method (on the workflow virtual thread).

```java
// Create a typed activity proxy (safe to store as a field)
MyActivities activities = Workflow.newActivityStub(MyActivities.class);
MyActivities activities = Workflow.newActivityStub(MyActivities.class, ActivityOptions.defaults());

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
        .taskQueue("heavy-tasks")      // optional; defaults to workflow's queue
        .startToCloseTimeout(Duration.ofSeconds(30))  // default: 10s
        .maxAttempts(3)                // default: 1
        .build();
```

---

## Package Structure

```
com.cajunsystems.boudin
├── annotation/         @WorkflowInterface, @WorkflowMethod, @SignalMethod,
│                       @QueryMethod, @ActivityInterface, @ActivityMethod
├── api/                WorkflowClient, WorkflowStub, Worker,
│                       WorkflowOptions, WorkflowFailureException
├── activity/           ActivityStub (InvocationHandler), ActivityOptions,
│                       ActivityFailureException
├── history/            HistoryEvent (sealed hierarchy), HistorySerializer
├── internal/           WorkflowDispatcher, ActivityDispatcher, WorkflowRunner,
│                       WorkflowRegistry, ActivityRegistry, ReplayState
├── serialization/      KryoSerializer (thread-safe Kryo pool)
└── workflow/           Workflow (static facade), WorkflowContext, WorkflowThread
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
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Run tests (uses an in-memory Gumbo persistence adapter — no disk I/O required):

```bash
mvn verify
```
