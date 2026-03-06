---
paths:
  - "**/*.py"
---

# Import rules

## Import order

Groups separated by blank line:
1. **stdlib** (`import os`, `from typing import ...`)
2. **third-party** (`import pendulum`, `import semver`)
3. **local dlt** (`from dlt.common import ...`)
4. (tests only) **test utilities** (`from tests.utils import ...`)

- Stdlib imports are ALWAYS at module level -- never inline in functions. This includes `contextlib`, `io`, `warnings`, `sys`, `os`, `re`, etc.
- `dlt.common` imports (except `dlt.common.libs`) are module level. They are always available and have no heavy side effects.
- `dlt.common.libs.*` imports may be inline -- they wrap optional dependencies that may not be installed (see Optional dependencies below).
- If a module is already imported at the top of the file, additional imports from the same module go at the top too -- never inline a second import from an already-imported module.
- No relative imports.

## Logging

`from dlt.common import logger`, then `logger.info(...)` -- never `import logging` directly (except in `logger.py` itself or runtime infrastructure).

## TYPE_CHECKING imports

- Optional deps for annotations only: `if TYPE_CHECKING: from X import Y` with `else: Y = Any`
- Destination factories: client class under `TYPE_CHECKING`, actual import deferred to runtime method

## Optional dependencies

dlt wraps optional deps in `dlt/common/libs/` with fail-fast `MissingDependencyException`. Always import from the wrapper, never from the package directly.

### Canonical patterns

```python
from dlt.common.libs.pandas import pandas
```

### Rules

- NEVER import **optional package** directly
- ALWAYS import from `dlt.common.libs.<optional package>`
- Import at the top of the module if module already imports **optional package**
- Import inline in other cases.

**Applies to**: `pyarrow`, `pandas`, `numpy`, `pydantic`, `sqlalchemy`, `ibis`, `deltalake`, `pyiceberg`
