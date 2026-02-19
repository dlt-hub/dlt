---
name: Module Hierarchy Check
description: Ensures proper module boundaries are maintained — dlt/common/ stays generic, protocols are used instead of concrete imports across boundaries, and circular/inner imports are avoided.
---

# Module Hierarchy Check

## Context

dlt maintains a clear module hierarchy to prevent circular dependencies and keep a clean architecture. The lead maintainer (`rudolfix`) frequently reviews for module boundary violations: "I'd love to keep such stuff out of the common", "use `SupportsPipeline` from common or create another protocol", "If we eliminate all inner imports in the code below we'll be more or less sure it is the case."

## Module Dependency Rules

The dependency flow should be:

```
dlt/common/  →  (no imports from dlt/extract, dlt/normalize, dlt/load, dlt/pipeline, dlt/destinations)
dlt/extract/ →  dlt/common/
dlt/normalize/ → dlt/common/
dlt/load/    →  dlt/common/
dlt/pipeline/ → dlt/common/, dlt/extract/, dlt/normalize/, dlt/load/
dlt/destinations/ → dlt/common/
dlt/dataset/ → dlt/common/, dlt/destinations/
dlt/_workspace/ → dlt/common/, dlt/extract/, dlt/pipeline/, dlt/destinations/, dlt/sources/
```

## What to Check

### 1. dlt/common/ Must Stay Generic

If the PR adds new imports to `dlt/common/`:

- Does it import from `dlt/extract/`, `dlt/normalize/`, `dlt/load/`, `dlt/pipeline/`, or `dlt/destinations/`?
- If so, this is a violation. Use a Protocol in `dlt/common/` instead.
- Destination-specific types should live in `dlt/destinations/`, not `dlt/common/`

### 2. Protocol Usage for Cross-Module Interfaces

When modules need to reference each other:

- Use `Protocol` classes (structural typing) from `dlt/common/` for interfaces
- Example: `SupportsPipeline` protocol instead of importing `Pipeline` directly

```python
# GOOD — protocol in common/
class SupportsPipeline(Protocol):
    def sync_destination(self) -> None: ...

# BAD — concrete import across boundary
from dlt.pipeline.pipeline import Pipeline
```
