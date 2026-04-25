# Integrations

## Primary: Gumbo Shared Append-Only Log

The sole external system Boudin integrates with. Gumbo is the persistence, coordination, and delivery layer.

**Dependency:** `com.cajunsystems:gumbo:main-SNAPSHOT` via JitPack

### Log Tags Used

| Tag Pattern | Written by | Read by | Purpose |
|-------------|-----------|---------|---------|
| `workflow-tasks:{taskQueue}` | `WorkflowStub` | `WorkflowDispatcher` | Workflow start events; task queue for workers |
| `workflow-history:{workflowId}` | `WorkflowStub`, `WorkflowThread`, `ActivityDispatcher`, `ActivityStub` | `WorkflowRunner`, `WorkflowStub` | Complete event history per workflow |
| `activity-tasks:{taskQueue}` | `ActivityStub` | `ActivityDispatcher` | Activity invocation queue |

### Key Gumbo APIs Used

- `SharedLogService.open(SharedLogConfig)` — open log with persistence adapter
- `SharedLog.append(AppendRequest)` — write events (single or multi-tag atomic)
- `LogView.subscribeAll()` — historical + live tail subscription (used for crash recovery)
- `LogView.subscribeTail()` — live events only subscription
- `LogView.readAll()` — read all historical events (used in crash recovery scans)

### Persistence Adapters (from Gumbo)

- `FileBasedPersistenceAdapter` — disk-backed production storage
- `InMemoryPersistenceAdapter` — ephemeral, used in tests

### Multi-Tag Atomic Appends

Critical operations write a single event visible on multiple subscriptions atomically:
```java
// ActivityStub.invoke() — one log write, visible to both history and task queue subscribers
sharedLog.append(AppendRequest.to(Set.of(historyTag, taskTag), serializedEvent))
```
This provides atomicity without distributed transactions.

## Serialization: Kryo

Used via `KryoSerializer` (thread-safe pool of 16 instances).

- Serializes: workflow arguments, activity arguments/results, signal payloads, `HistoryEvent` objects
- No type registration required (`setRegistrationRequired(false)`)
- Supports generics, nested objects, collections, arbitrary POJOs
- Thread-safe via object pooling

## No Other External Systems

Boudin has no direct integration with:
- **Databases** — log is the only store
- **Message brokers** (Kafka, RabbitMQ, NATS) — log subscriptions serve as task queues
- **Redis / caches** — no caching layer
- **HTTP / REST APIs** — no outbound HTTP from the framework itself
- **Spring / Quarkus / Micronaut** — standalone library, no framework dependency

## User-Provided Integrations (via Activities)

Activities are the integration point for external systems:
```java
@ActivityInterface
public interface OrderActivities {
    @ActivityMethod
    PaymentResult chargePayment(Order order); // implementation calls payment API
}
```
Activities run on virtual threads — blocking I/O (HTTP, JDBC, gRPC) is safe.

## Build / Distribution

- **JitPack** — resolves Gumbo from GitHub; requires internet access at build time
- **Maven Central** — all other dependencies (JUnit, SLF4J, AssertJ, Awaitility)
- Boudin itself: snapshot artifact, not yet published to Maven Central
