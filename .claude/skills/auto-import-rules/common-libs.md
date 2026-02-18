# Optionals deps from wrappers

dlt wraps optional deps in `dlt/common/libs/` with fail-fast `MissingDependencyException`. Always import from the wrapper, never from the package directly.

## Canonical patterns

```python
from dlt.common.libs.pandas import pandas
```

## Rules

- NEVER import **optional package** directly
- ALWAYS import from `dlt.common.libs.<optional package>`
- Import at the top of the module if module already imports **optional package**
- Import inline in other cases.
