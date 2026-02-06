---
name: Deprecation Guidelines Check
description: Ensures that breaking changes follow dlt's deprecation policy — using DltDeprecationWarning with version tracking, the @deprecated decorator, backward compatibility tests, and proper migration paths.
---

# Deprecation Guidelines Check

## Context

dlt introduces breaking changes only in major versions. All removals or behavioral changes must go through a deprecation cycle as documented in `CONTRIBUTING.md`. The project provides infrastructure in `dlt/common/warnings.py` for this purpose.

## What to Check

### 1. Removed or Renamed Public APIs

If the PR removes or renames any public function, class, method, parameter, or constant:

- **Is a deprecation warning emitted for the old name/API?** It should use `DltDeprecationWarning` or a version-specific variant (e.g., `Dlt100DeprecationWarning`).
- **Is the old API still accessible?** The old name should still work but emit a warning.
- **Does the warning include `since` and `expected_due` versions?** Every deprecation warning must specify when it was deprecated and when removal is planned.

```python
# GOOD deprecation pattern
from dlt.common.warnings import D100DeprecationWarning
import warnings

warnings.warn(
    "old_function is deprecated, use new_function instead",
    Dlt100DeprecationWarning,
    stacklevel=2,
)
```

### 2. @deprecated Decorator Usage

For deprecated classes, functions, or overloads, the `@deprecated` decorator (PEP 702) from `dlt/common/warnings.py` should be used:

```python
from dlt.common.warnings import deprecated

@deprecated("Use new_function instead")
def old_function(): ...
```

### 3. Schema/State Migrations

If the PR changes schema structure or pipeline state format:

- Is the `engine_version` bumped?
- Are migration methods provided in the relevant module?
- Do existing schemas/states still load correctly?

### 4. Backward Compatibility Tests

If the PR deprecates functionality:

- Are there tests verifying the old API still works (with warnings)?
- Consider whether `tests_dlt_versions.py` end-to-end tests need updating.

### 5. Configuration Changes

If configuration keys are renamed or removed:

- Is the old key still accepted with a deprecation warning?
- Are TOML config files and environment variable names backward compatible?
