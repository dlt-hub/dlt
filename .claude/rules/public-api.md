---
paths:
  - "dlt/**/*.py"
---

# Public API and backward compatibility

## Public interface
- Everything DEFINED and DIRECTLY IMPORTED in @dlt/__init__.py is PUBLIC INTERFACE. Change here is BREAKING CHANGE.
- Behaviors described in @docs are considered public and changing them is a BREAKING CHANGE (in outputs of PUBLIC INTERFACES)
- Always flag BREAKING CHANGES clearly with # BREAKING: comments

## Deprecation warnings
- Use `DltDeprecationWarning` from `dlt/common/warnings.py`, never raw `DeprecationWarning`
- Pass the `since` version where the deprecation is introduced
- Use `Dlt100DeprecationWarning` only for features deprecated since 1.0.0 (hardcodes `since`)
- To deprecate entire functions or classes, use the `deprecated` decorator from `dlt/common/warnings.py`

## Backward compatibility mechanisms
- Avoid BREAKING CHANGES with deprecations, configuration options etc.
- Schema/state migration via `engine_version`, versioned storage layouts, deprecation warnings, `deprecated` decorator
- Backward compat must be tested -- see `tests/pipeline/test_dlt_versions.py` for e2e version upgrade tests
