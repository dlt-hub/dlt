---
sidebar_label: signals
title: common.runtime.signals
---

## signal\_received

```python
def signal_received() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/runtime/signals.py#L35)

check if a signal was received

## sleep

```python
def sleep(sleep_seconds: float) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/runtime/signals.py#L40)

A signal-aware version of sleep function. Will raise SignalReceivedException if signal was received during sleep period.

## delayed\_signals

```python
@contextmanager
def delayed_signals() -> Iterator[None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/runtime/signals.py#L51)

Will delay signalling until `raise_if_signalled` is used or signalled `sleep`

