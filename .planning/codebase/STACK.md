# Stack

## Language & Runtime

| Item | Value |
|------|-------|
| **Language** | Java 21 |
| **Build** | Maven 3.x |
| **JVM Features Used** | Virtual threads (Project Loom), Sealed interfaces, Records |
| **Maven Compiler Plugin** | 3.13.0 (source/target: Java 21) |
| **Maven Surefire Plugin** | 3.2.5 |

## Core Dependencies

| Dependency | Version | Purpose |
|-----------|---------|---------|
| **gumbo** (CajunSystems) | main-SNAPSHOT | Shared append-only log — the persistence and coordination layer |
| **Kryo** | (via gumbo) | Fast binary serialization for POJOs, generics, collections |
| **SLF4J API** | 2.0.12 | Logging facade |
| **Logback Classic** | 1.5.3 | SLF4J implementation |

## Test Dependencies

| Dependency | Version | Purpose |
|-----------|---------|---------|
| **JUnit Jupiter** | 5.10.2 | Test framework |
| **AssertJ Core** | 3.25.3 | Fluent assertions |
| **Awaitility** | 4.2.1 | Async polling in tests |

## Key Language Features

- **Virtual Threads**: Every workflow and activity runs on a Java 21 virtual thread — cheap, scalable, safe for blocking I/O
- **Sealed Interfaces**: `HistoryEvent` sealed hierarchy with 9 record implementations — exhaustive pattern matching
- **Records**: All event types are immutable records (`WorkflowStarted`, `ActivityCompleted`, etc.)
- **Dynamic Proxies**: `java.lang.reflect.Proxy` backs all workflow/activity stubs

## Distribution

- Gumbo dependency resolved via JitPack (GitHub-based artifact repository)
- Boudin itself is snapshot-only; not yet published to Maven Central
- CI runs on GitHub Actions with Ubuntu + Java 21 Temurin
