---
sidebar_label: signals
title: common.runtime.signals
---

## signal\_received

```python
def signal_received() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/signals.py#L35)

check if a signal was received

## sleep

```python
def sleep(sleep_seconds: float) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/signals.py#L40)

A signal-aware version of sleep function. Will raise SignalReceivedException if signal was received during sleep period.

## wake\_all

```python
def wake_all() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/signals.py#L51)

Wakes all threads sleeping on event

## delayed\_signals

```python
@contextmanager
def delayed_signals() -> Iterator[None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/signals.py#L57)

Will delay signalling until `raise_if_signalled` is used or signalled `sleep`

