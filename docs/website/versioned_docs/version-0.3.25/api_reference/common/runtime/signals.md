---
sidebar_label: signals
title: common.runtime.signals
---

## sleep

```python
def sleep(sleep_seconds: float) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/runtime/signals.py#L39)

A signal-aware version of sleep function. Will raise SignalReceivedException if signal was received during sleep period.

## delayed\_signals

```python
@contextmanager
def delayed_signals() -> Iterator[None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/runtime/signals.py#L50)

Will delay signalling until `raise_if_signalled` is used or signalled `sleep`

