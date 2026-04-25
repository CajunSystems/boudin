# Structure

## Directory Layout

```
boudin/
├── pom.xml                         — Maven build (Java 21, JitPack for gumbo)
├── README.md                       — Project overview, architecture, developer guide
├── LICENSE
├── .github/workflows/ci.yml        — GitHub Actions CI (build + test on all pushes)
└── src/
    ├── main/java/com/cajunsystems/boudin/
    │   ├── annotation/             — Declarative annotations (@WorkflowInterface, etc.)
    │   ├── api/                    — Public user-facing API
    │   ├── workflow/               — Workflow execution runtime
    │   ├── activity/               — Activity execution model
    │   ├── internal/               — Dispatchers, registries, replay (not public)
    │   ├── history/                — Event model and serialization
    │   └── serialization/          — Kryo wrapper
    └── test/java/com/cajunsystems/boudin/
        └── GreetingWorkflowTest.java — Integration tests
```

## Package Responsibilities

### `annotation/`
Pure marker annotations — no Boudin dependencies.
- `@WorkflowInterface`, `@WorkflowMethod`, `@SignalMethod`, `@QueryMethod`
- `@ActivityInterface`, `@ActivityMethod`
- All use `@Retention(RUNTIME)` — required for reflection-based discovery

### `api/`
User-facing entry points. Only package users need to directly import (plus `annotation`).
- `WorkflowClient` — creates typed workflow stubs; client entry point
- `WorkflowStub` — `InvocationHandler` backing client proxies
- `SignalOnlyStub` — targets existing workflow by ID for signals only
- `Worker` + builder — server-side entry point; manages dispatchers
- `WorkflowOptions` + builder — workflow execution config (taskQueue, workflowId, timeout)
- `WorkflowFailureException` — thrown when a workflow execution fails

### `workflow/`
Workflow execution runtime.
- `Workflow` — static facade: `newActivityStub()`, `await()`, `getWorkflowId()`
- `WorkflowContext` — per-instance state holder: activity futures, signal lock, replay state
- `WorkflowThread` — runs workflow method on a virtual thread; lifecycle management

### `activity/`
Activity invocation model.
- `ActivityStub` — `InvocationHandler`; handles replay vs. live activity scheduling
- `ActivityOptions` + builder — activity config (taskQueue, timeout, maxAttempts)
- `ActivityFailureException` — thrown when an activity permanently fails

### `internal/`
Core machinery — not intended for direct user use.
- `WorkflowDispatcher` — subscribes to `workflow-tasks:{q}`; creates `WorkflowRunner` per execution
- `WorkflowRunner` — per-workflow lifecycle: history load, replay, live event routing
- `WorkflowRegistry` — maps workflow type names → implementations and annotated methods
- `ActivityDispatcher` — subscribes to `activity-tasks:{q}`; executes activities
- `ActivityRegistry` — maps activity type names → implementations and methods
- `ReplayState` — pre-loaded activity result cache; deterministic ID sequence counters

### `history/`
Immutable event model. Used by all other packages.
- `HistoryEvent` — sealed interface; 9 record implementations:
  `WorkflowStarted`, `WorkflowCompleted`, `WorkflowFailed`,
  `ActivityScheduled`, `ActivityCompleted`, `ActivityFailed`,
  `SignalReceived`, `TimerStarted`, `TimerFired`
- `HistorySerializer` — Kryo-based serializer for `HistoryEvent` polymorphism

### `serialization/`
Thin wrapper over Kryo. No Boudin dependencies.
- `KryoSerializer` — thread-safe Kryo with object pool (size 16); serializes arbitrary POJOs

## Package Dependency Direction

```
annotation  ←  (no deps)
serialization  ←  Kryo only
history  ←  serialization
workflow  ←  activity, history, serialization, internal
activity  ←  workflow, history, serialization, internal
internal  ←  history, serialization, workflow, annotation
api  ←  workflow, activity, history, internal, serialization, annotation
```

## File Count

| Package | Files |
|---------|-------|
| annotation | 6 |
| api | 6 |
| workflow | 3 |
| activity | 3 |
| internal | 6 |
| history | 2 |
| serialization | 1 |
| **Total** | **27** |
| test | 1 |
