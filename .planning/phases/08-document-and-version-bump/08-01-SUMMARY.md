# Phase 8, Plan 1: Documentation and Version Bump — Summary

## Status: Complete

## What Was Built

Updated `README.md` to comprehensively document all Milestone 1 APIs including durable timers,
child workflows, observability, and the full `ActivityOptions` retry policy. Created `CHANGELOG.md`
recording all features delivered across Phases 1–7. Bumped the project version from `0.1.0-SNAPSHOT`
to `0.1.0` in `pom.xml`, completing the Milestone 1 release.

## Tasks Completed

| Task | Commit | Description |
|------|--------|-------------|
| 1 — Update README.md | `0ce23e7` | Added Concepts rows (Timer, Child Workflow), updated HistoryEvent hierarchy to 13 events, expanded Workflow static facade with sleep/child stubs, expanded ActivityOptions with retry fields + formula, added Durable Timers section, Child Workflows section, Observability section (Metrics table + MDC table), updated Package Structure, ToC, and Building version |
| 2 — Create CHANGELOG.md | `dc67d4c` | Created CHANGELOG.md documenting all Milestone 1 features across core engine, activity enforcement, durable timers, child workflows, observability, and correctness |
| 3 — Bump version to 0.1.0 | `e3d4665` | Changed pom.xml version from `0.1.0-SNAPSHOT` to `0.1.0`; verified with `mvn help:evaluate` |

## Key Changes

- README sections added: Durable Timers, Child Workflows, Observability (Metrics + MDC Context)
- README sections updated: Concepts table, HistoryEvent hierarchy, Workflow static facade, ActivityOptions, Package Structure, Table of Contents, Building version
- CHANGELOG covers: Milestone 1 (phases 1-7) — core engine, activity enforcement, durable timers, child workflows, observability, correctness
- Version: 0.1.0-SNAPSHOT → 0.1.0

## Test Results

All 27 tests pass (mvn test) — unchanged

## Issues / Deviations

None
