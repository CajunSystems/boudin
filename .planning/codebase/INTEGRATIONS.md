# Integrations

## Primary: Gumbo Shared Append-Only Log

The sole external system Boudin integrates with. Gumbo is the persistence, coordination, and delivery layer.

**Dependency:** `com.github.CajunSystems:gumbo:0.2.0` via JitPack

### Log Tags Used

| Tag Pattern | Written by | Read by | Purpose |
|-------------|-----------|---------|---------|
| `workflow-tasks:{taskQueue}` | `WorkflowStub` | `WorkflowDispatcher` | Workflow start events; task queue for workers |
| `workflow-history:{workflowId}` | `WorkflowStub`, `WorkflowThread`, `ActivityDispatcher`, `ActivityStub` | `WorkflowRunner`, `WorkflowStub` | Complete event history per workflow |
| `activity-tasks:{taskQueue}` | `ActivityStub` | `ActivityDispatcher` | Activity invocation queue |

### Key Gumbo APIs Used (0.2.0)

- `SharedLogService.open(SharedLogConfig)` — open log with persistence adapter
- `SharedLog.append(AppendRequest)` — write events (single or multi-tag atomic)
- `SharedLog.appendBatch(List<AppendRequest>)` — batch append (reduces sequencer RTTs)
- `LogView.subscribe(from, listener)` — historical + live tail subscription
- `LogView.subscribeTail(listener)` — live events only subscription (no backlog)
- `LogView.readAll()` — read all historical events
- `LogView.readFrom(position, max)` — O(log N) positional read
- `LogView.readAfter(seqnum)` — delta reads for efficient catch-up
- `LogView.getValue(key)` / `LogView.setValue(key, bytes)` — **NEW: per-tag KV store**
- `LogView.getLatestSeqnum()` — O(1) tail position query
- `TypedLogView<T>` — type-safe wrapper with Kryo serialization
- `ExecutorEngine` — **NEW: stateless executor model** for crash-safe processing
- `Executor<S>` — **NEW: pure fold function** (state + entry → new state → append results)

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
