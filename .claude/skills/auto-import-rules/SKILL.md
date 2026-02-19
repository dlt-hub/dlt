---
name: auto-import-rules
description: Enforces dlt import conventions and ordering. Use when writing or modifying Python imports in dlt/ or tests/.
user-invocable: false
---

# Import rules

## Import order

Groups separated by blank line:
1. **stdlib** (`import os`, `from typing import ...`)
2. **third-party** (`import pendulum`, `import semver`)
3. **local dlt** (`from dlt.common import ...`)
4. (tests only) **test utilities** (`from tests.utils import ...`)

- Stdlib imports are ALWAYS at module level -- never inline in functions. This includes `contextlib`, `io`, `warnings`, `sys`, `os`, `re`, etc.
- No relative imports.

## Logging

`from dlt.common import logger`, then `logger.info(...)` -- never `import logging` directly (except in `logger.py` itself or runtime infrastructure).

## TYPE_CHECKING imports

- Optional deps for annotations only: `if TYPE_CHECKING: from X import Y` with `else: Y = Any`
- Destination factories: client class under `TYPE_CHECKING`, actual import deferred to runtime method

## Optional dependencies

**When importing `pyarrow`, `pandas`, `numpy`, `pydantic`, `sqlalchemy`, `ibis`, `deltalake`, `pyiceberg`**: See [common-libs.md](common-libs.md)
