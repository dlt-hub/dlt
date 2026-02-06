---
name: Exception Handling Check
description: Verifies that new exceptions follow dlt's hierarchy (DltException base, Terminal/Transient mixins), error messages include actionable context, and exception types are semantically correct.
---

# Exception Handling Check

## Context

dlt uses a structured exception hierarchy rooted at `DltException` with mixin classes that control retry behavior. The distinction between `TerminalException` (unrecoverable) and `TransientException` (retryable) is critical for the pipeline's error handling and retry logic.

## Exception Hierarchy

From `dlt/common/exceptions.py`:

```
DltException (base)
├── TerminalException (mixin) — cannot be recovered from, pipeline stops
├── TransientException (mixin) — can be retried
├── TerminalValueError(ValueError, TerminalException)
├── SignalReceivedException(KeyboardInterrupt, TerminalException)
└── ... many domain-specific exceptions
```

Each domain has its own exception module:
- `dlt/pipeline/exceptions.py`
- `dlt/destinations/exceptions.py`
- `dlt/common/configuration/exceptions.py`
- `dlt/common/storages/exceptions.py`
- `dlt/normalize/exceptions.py`
- `dlt/destinations/impl/<name>/exceptions.py`

## What to Check

### 1. New Exception Classes

- Should extend `DltException` (directly or transitively)
- Must include the correct mixin: `TerminalException` for unrecoverable errors, `TransientException` for retryable ones
- Should be placed in the appropriate domain's `exceptions.py` file

```python
# GOOD
class MyDestinationError(DestinationException, TerminalException):
    def __init__(self, table_name: str) -> None:
        super().__init__(f"Failed to load table {table_name}")

# BAD — generic Exception, no terminal/transient classification
class MyDestinationError(Exception):
    pass
```

### 2. Error Specificity

- Use `ValueError` (or `TerminalValueError`) for invalid inputs that can't be coerced — not generic `Exception`
- Use `KeyError` or `ConfigFieldMissingException` for missing configuration
- Use `NotImplementedError` only for abstract methods that must be overridden
- Don't raise `Exception` or `RuntimeError` directly — create a specific exception class

### 3. Error Messages

- Include relevant context (table name, column name, destination name, file path)
- Reference the user action that caused the error when possible
- For configuration errors, include the provider locations that were searched (see `ConfigFieldMissingException` pattern)

### 4. Exception Pickling

Exceptions that might cross process boundaries (e.g., in parallel test execution with xdist) should be picklable. `DltException` provides a `__reduce__` method for this — custom exceptions with extra constructor parameters should follow this pattern.

### 5. Return Type Annotations

If a function can return `None`, the type annotation should be `Optional[T]`, not just `T`. This is a frequent review finding.
