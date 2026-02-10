---
paths:
  - "**/*.py"
---

# Coding style

## Formatting
- Line length: 100 chars (black + isort, profile "black")
- `NOTE:` and `TODO:` prefixes are uppercase (`# NOTE: this may change`)
- f-strings for formatting, `.format()` only for runtime templates

## Code reuse
- Obvious utils and patterns are already in code base
- When in doubt look at the code around the changes you are making - not only style but coding patterns and code organization
- When using a util function or any other function from the code base - look at existing usage if in doubt how it works

# Imports
Use the `auto-import-rules` skill for full conventions including optional dependency wrappers.

# Docstrings
Use the `auto-docstring-rules` skill for full conventions including public API, internal, and config field docstrings.

## Comments
- Inline comments start with lowercase (`# resolve the schema`)
- NEVER add comments that separate code blocks (`# -- sqlglot_dialect mapping --`)
- DO NOT state the obvious, do not document your thought process

## Types and data structures
- Always use full generic parametrization -- `Dict[str, Any]` not `dict`
- Type aliases use T-prefix PascalCase (`TDataItem`, `TLoaderFileFormat`) -- see @dlt/common/typing.py
- `TypedDict` for structured dicts, `NamedTuple` for lightweight immutable data

## Exceptions
- Inherit from `DltException`; use `TerminalException`/`TransientException` mixins -- see @dlt/common/exceptions.py
