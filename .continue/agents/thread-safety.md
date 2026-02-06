---
name: Thread Safety Check
description: Reviews code that runs in concurrent contexts (load jobs, sinks, parallel extraction) for thread safety issues — shared mutable state, non-atomic operations, and missing synchronization.
---

# Thread Safety Check

## Context

dlt processes data in parallel at multiple stages. Load jobs run across multiple threads, sink destinations are called from many threads simultaneously, and `pytest-xdist` runs tests in parallel processes. Thread safety issues are a recurring review concern (e.g., "make sure this code is thread safe! sinks are called from many threads and work in parallel").

## Critical Concurrent Code Paths

1. **Load jobs** (`dlt/load/load.py`) — Multiple load jobs execute in parallel threads
2. **Sink/destination functions** (`dlt/destinations/impl/sink/`) — Called from many threads
3. **Pipeline state** (`dlt/common/pipeline.py`) — Accessed during parallel operations
4. **Load package state** (`dlt/common/storages/load_package.py`) — Shared across load jobs
5. **Collector/progress tracking** (`dlt/common/runtime/collector_base.py`) — Updated from parallel threads

## What to Check

### 1. Shared Mutable State

If the PR modifies code in any of the concurrent paths above:

- Are class-level or module-level mutable variables (dicts, lists, sets) being modified?
- Is `dict.setdefault()` or similar non-atomic read-modify-write used without locking?
- Are there race conditions between checking a condition and acting on it?

### 2. State Access in Sinks

For changes to sink destinations or custom destination functions:

- Is state being written from multiple threads without synchronization?
- Are file handles or database connections shared across threads?
- Is batch processing state properly isolated per-thread?

### 3. Pipeline State Modifications

For changes to pipeline state management:

- Is `pipeline_state()` accessed thread-safely?
- Are state mutations protected by locks where needed?
- Is state persisted atomically (not partially written)?

### 4. Container and Injection

For changes to `dlt/common/configuration/container.py`:

- The Container is a singleton — are injected contexts thread-safe?
- Are context managers properly scoped per-thread?

### 5. Test Parallelization Safety

For new or modified tests:

- Do tests use unique pipeline names (via `uniq_id()`)?
- Do tests use `get_test_storage_root()` for isolated storage?
- Are tests that can't run in parallel marked with `@pytest.mark.serial`?
