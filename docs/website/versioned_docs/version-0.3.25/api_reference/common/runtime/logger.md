---
sidebar_label: logger
title: common.runtime.logger
---

## \_\_getattr\_\_

```python
def __getattr__(name: str) -> LogMethod
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/runtime/logger.py#L20)

Forwards log method calls (debug, info, error etc.) to LOGGER

## metrics

```python
def metrics(name: str, extra: StrAny, stacklevel: int = 1) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/runtime/logger.py#L35)

Forwards metrics call to LOGGER

