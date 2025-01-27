---
sidebar_label: logger
title: common.logger
---

## \_\_getattr\_\_

```python
def __getattr__(name: str) -> LogMethod
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/logger.py#L14)

Forwards log method calls (debug, info, error etc.) to LOGGER

## metrics

```python
def metrics(name: str, extra: Mapping[str, Any], stacklevel: int = 1) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/logger.py#L29)

Forwards metrics call to LOGGER

