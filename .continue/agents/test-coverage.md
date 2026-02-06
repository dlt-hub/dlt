---
name: Test Coverage Check
description: Ensures new features and bug fixes include appropriate tests, tests follow dlt's parallel-safe patterns, and test organization mirrors the source structure.
---

# Test Coverage Check

## Context

dlt has ~3,100+ tests organized in `tests/` mirroring the `dlt/` source structure. The project uses pytest with xdist for parallel execution. CONTRIBUTING.md states that "significant changes require tests and documentation" and "writing tests will often be more time-consuming than writing the code."

## What to Check

### 1. New Features Must Have Tests

If the PR adds new functionality:

- Are there corresponding tests?
- Do tests cover both happy path and error cases?
- For new destination features: are edge cases like "missing column in data" tested?
- For new API parameters: is each new option exercised in tests?

### 2. Bug Fixes Should Have Regression Tests

If the PR fixes a bug:

- Is there a test that would have caught the bug before the fix?
- Does the test verify the specific scenario from the bug report?

### 3. Test File Organization

Tests should mirror the source structure:

| Source | Test Location |
|---|---|
| `dlt/extract/` | `tests/extract/` |
| `dlt/normalize/` | `tests/normalize/` |
| `dlt/load/` | `tests/load/` |
| `dlt/pipeline/` | `tests/pipeline/` |
| `dlt/common/` | `tests/common/` |
| `dlt/destinations/impl/<name>/` | `tests/load/` (destination tests) |
| `dlt/sources/<name>/` | `tests/sources/` |

### 4. Parallel Test Safety

All new tests must be safe for parallel execution with pytest-xdist:

- **Unique pipeline names**: Use `uniq_id()` from `dlt.common.utils` in pipeline names
- **Isolated storage**: Use `get_test_storage_root()` from `tests/utils.py`
- **No shared global state**: Tests must not depend on execution order
- **Serial marker**: Tests that cannot run in parallel must be decorated with `@pytest.mark.serial`
- **Forked marker**: Tests that modify global state should use `@pytest.mark.forked`

```python
# GOOD
from dlt.common.utils import uniq_id

def test_my_feature():
    pipeline = dlt.pipeline(
        pipeline_name=f"test_my_feature_{uniq_id()}",
        destination="duckdb",
    )
```

### 5. Test Markers

Appropriate markers should be used:

- `@pytest.mark.essential` — For critical test paths
- `@pytest.mark.serial` — Tests that can't run in parallel
- `@pytest.mark.forked` — Tests requiring process isolation
- `@pytest.mark.no_load` — Tests that don't load data to destinations

### 6. Configuration Provider Patching

Tests should not accidentally use production configuration providers. The root `tests/conftest.py` patches out Google/Airflow secrets providers. New test conftest files should follow the same pattern.

### 7. Active Destinations Pattern

Destination tests should use the `ACTIVE_DESTINATIONS` / `ALL_FILESYSTEM_DRIVERS` environment variable pattern to allow selective test execution.
