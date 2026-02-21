---
paths:
  - "tests/**/*.py"
  - "tools/tests/**/*.py"
---

# Testing

## Style and structure
- Module-based tests (plain `def test_*() -> None:` functions), not class-based
- pytest with fixtures in `conftest.py`, not unittest setUp/tearDown

## Reduce tests sprawl
- Use `pytest.mark.parametrize` to reduce code duplication. ALWAYS use human readable ids for the test cases
- It is OK to test many related use cases in one test: ie. when they share common setup or represent a flow of user actions

## Fixtures
- We have autouse fixtures enabled by `conftest.py`. Always check those before adding common fixtures ie. to preserve env variables or get clean storage for the tests
- We have A LOT of fixtures in files like @tests/utils.py and @tests/load/utils.py, look there before adding new ones.

## Destination tests (tests/load)
- Tests in @tests/load are ALWAYS run on CI in context of particular destination. Tests that run pipelines MUST use `destinations_configs`, other test modules must filter for a right destination (ie. `skip_if_not_active("clickhouse")`)
- NOTE: tests outside @tests/load do not use such filtering. Use `duckdb`, `dummy` or `sqlite` or `filesystem` destinations for such tests.

### Test Markers
- `@pytest.mark.essential` — For critical test paths
- `@pytest.mark.serial` — Tests that can't run in parallel

## Parallel safety and isolation
- Tests run in parallel! ALWAYS use test storage (`get_test_storage`), unique pipeline names (`uniq_id`), and worker-aware storage roots (`get_test_storage_root` from `tests/utils.py`). Isolate pipelines with `dev_mode` or random `dataset_name`. Avoid external resources that clash with other tests.
- Tests must work on Linux, macOS and Windows. Avoid encoding paths and platform-specific code.

## Placement and comments
- When adding new test ALWAYS try find the right place by looking where code you make changes is tested
- Docstrings: DESCRIBE test case. SKIP when test name is sufficient
- Use line comments to describe and separate use cases within a single test
