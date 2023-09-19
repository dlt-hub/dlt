---
sidebar_label: logger
title: common.runtime.logger
---

#### \_\_getattr\_\_

```python
def __getattr__(name: str) -> LogMethod
```

Forwards log method calls (debug, info, error etc.) to LOGGER

#### metrics

```python
def metrics(name: str, extra: StrAny, stacklevel: int = 1) -> None
```

Forwards metrics call to LOGGER

