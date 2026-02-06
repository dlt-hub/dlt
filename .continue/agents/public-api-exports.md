---
name: Public API Exports Check
description: Verifies that new public-facing classes, functions, or types are properly exported via __all__ in the appropriate __init__.py files, and that the top-level dlt public API surface remains intentional.
---

# Public API Exports Check

## Context

dlt carefully controls its public API surface through `__all__` definitions in `__init__.py` files. The top-level `dlt/__init__.py` exports the core public API (source, resource, transformer, destination, pipeline, etc.). Sub-packages like `dlt/destinations/__init__.py` also maintain their own `__all__`.

A common review finding is forgetting to add new public items to `__all__` (e.g., "we forgot to add `sink` to `__all__`").

## What to Check

1. **New public classes/functions in `dlt/`**: If the PR adds a new class, function, decorator, or type alias that is intended for user consumption, verify it is added to the appropriate `__all__` list.

2. **New destination implementations**: If a new destination is added under `dlt/destinations/impl/`, check that:
   - It's registered in `dlt/destinations/__init__.py`
   - The factory function is exported
   - Any user-facing configuration types are accessible

3. **New source implementations**: Similarly for `dlt/sources/`, ensure new sources are exported.

4. **Removed exports**: If something is removed from `__all__`, verify this is intentional and follows the deprecation guidelines (items should be deprecated first, not silently removed).

5. **Internal vs public**: Items prefixed with `_` are internal by convention. Verify that new public items don't accidentally expose internal implementation details.

## Key Files to Check

- `dlt/__init__.py` — Top-level public API
- `dlt/destinations/__init__.py` — Destination exports
- `dlt/sources/__init__.py` — Source exports
- `dlt/common/schema/typing.py` — Schema type exports
- `dlt/common/typing.py` — Common type exports
