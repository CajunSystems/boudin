# Conventions

## Naming

| Element | Convention | Example |
|---------|-----------|---------|
| Packages | domain-driven nouns | `api`, `internal`, `history` |
| Interfaces (user-defined) | noun + no suffix | `GreetingWorkflow`, `OrderActivities` |
| Implementations | interface name + `Impl` | `GreetingWorkflowImpl` |
| Proxy handlers | noun + `Stub` | `WorkflowStub`, `ActivityStub` |
| Dispatchers | noun + `Dispatcher` | `WorkflowDispatcher`, `ActivityDispatcher` |
| Registries | noun + `Registry` | `WorkflowRegistry`, `ActivityRegistry` |
| Constants | UPPER_SNAKE_CASE | `CURRENT_CONTEXT` |
| Instance fields | camelCase, private | `sharedLog`, `pendingActivities` |
| Getters | no `get` prefix | `.taskQueue()`, `.workflowId()` |
| Static factories | `newInstance()`, `newBuilder()` | `WorkflowClient.newInstance(log)` |

## Builder Pattern

All configuration objects use fluent builders:
```java
WorkflowOptions.newBuilder()
    .taskQueue("orders")
    .workflowId("order-123")
    .workflowRunTimeout(Duration.ofHours(24))
    .build();
```
Used in: `WorkflowOptions`, `ActivityOptions`, `Worker`.

## Sealed Records for Events

All history events are immutable records in a sealed hierarchy:
```java
public sealed interface HistoryEvent permits
    HistoryEvent.WorkflowStarted, HistoryEvent.ActivityCompleted, ...
{
    record WorkflowStarted(String eventId, Instant timestamp, ...) implements HistoryEvent {}
    record ActivityCompleted(...) implements HistoryEvent {}
    // ...
}
```

## Lazy Context Resolution

Activity stubs resolve `WorkflowContext` on each invocation, not at creation:
```java
// In ActivityStub.invoke()
WorkflowContext context = Workflow.currentContext();  // ThreadLocal lookup per call
```
This allows stubs to be created in field initializers before the workflow thread starts.

## Dynamic Proxy Pattern

All stubs are `InvocationHandler` implementations, created via `Proxy.newProxyInstance()`:
```java
(T) Proxy.newProxyInstance(
    activityInterface.getClassLoader(),
    new Class<?>[]{activityInterface},
    new ActivityStub(activityInterface, options)
);
```

## Error Handling

- `ActivityFailureException(activityId, errorType)` — wraps activity failures from history
- `WorkflowFailureException` — wraps workflow execution failures
- `IllegalStateException` — used for calls outside workflow thread (includes thread name in message)
- Reflection errors: `InvocationTargetException` unwrapped to get actual cause
- Serialization errors: propagated as-is (no custom wrapping)

## Threading Conventions

- Workflow and activity code runs on virtual threads (`Thread.ofVirtual()`)
- Thread naming: `boudin-workflow-{workflowId}` for debuggability
- `ConcurrentHashMap` for all shared mutable maps
- `volatile` for fields mutated by signal methods (e.g., `volatile String locale`)
- `synchronized (signalLock) { signalLock.wait() }` pattern for condition waiting

## Code Organization Within Files

1. Static fields and `ThreadLocal`s
2. Immutable identity/config fields
3. Mutable state fields
4. Constructor
5. Public API methods (grouped by concern)
6. Package-private helpers
7. Private implementation methods

Section demarcation uses `// ──` markers:
```java
// ── Activity coordination ────────────────────────
// ── Signal waiting ───────────────────────────────
```

## Documentation

- All public classes and methods have Javadoc
- Complex classes use `<h2>` section headers in class-level Javadoc
- Inline comments explain *why*, not *what*
- Non-obvious invariants commented (e.g., "Do NOT capture the context here — the stub may be created in a field initializer")
