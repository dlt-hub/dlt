---
sidebar_label: logger
title: common.logger
---

## \_\_getattr\_\_

```python
def __getattr__(name: str) -> LogMethod
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/logger.py#L15)

Forwards log method calls (debug, info, error etc.) to LOGGER

## metrics

```python
def metrics(name: str, extra: Mapping[str, Any], stacklevel: int = 1) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/logger.py#L30)

Forwards metrics call to LOGGER

