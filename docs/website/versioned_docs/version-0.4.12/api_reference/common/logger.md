---
sidebar_label: logger
title: common.logger
---

## \_\_getattr\_\_

```python
def __getattr__(name: str) -> LogMethod
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/logger.py#L15)

Forwards log method calls (debug, info, error etc.) to LOGGER

## metrics

```python
def metrics(name: str, extra: Mapping[str, Any], stacklevel: int = 1) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/logger.py#L30)

Forwards metrics call to LOGGER

