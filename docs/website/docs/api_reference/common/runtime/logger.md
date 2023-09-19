---
sidebar_label: logger
title: common.runtime.logger
---

#### \_\_getattr\_\_

```python
def __getattr__(name: str) -> LogMethod
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/logger.py#L22)

Forwards log method calls (debug, info, error etc.) to LOGGER

#### metrics

```python
def metrics(name: str, extra: StrAny, stacklevel: int = 1) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/logger.py#L35)

Forwards metrics call to LOGGER

