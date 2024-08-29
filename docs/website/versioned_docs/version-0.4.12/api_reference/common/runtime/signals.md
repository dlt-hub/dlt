---
sidebar_label: signals
title: common.runtime.signals
---

## signal\_received

```python
def signal_received() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/runtime/signals.py#L35)

check if a signal was received

## sleep

```python
def sleep(sleep_seconds: float) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/runtime/signals.py#L40)

A signal-aware version of sleep function. Will raise SignalReceivedException if signal was received during sleep period.

## delayed\_signals

```python
@contextmanager
def delayed_signals() -> Iterator[None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/runtime/signals.py#L51)

Will delay signalling until `raise_if_signalled` is used or signalled `sleep`

