---
sidebar_label: signals
title: common.runtime.signals
---

#### sleep

```python
def sleep(sleep_seconds: float) -> None
```

A signal-aware version of sleep function. Will raise SignalReceivedException if signal was received during sleep period.

#### delayed\_signals

```python
@contextmanager
def delayed_signals() -> Iterator[None]
```

Will delay signalling until `raise_if_signalled` is used or signalled `sleep`

