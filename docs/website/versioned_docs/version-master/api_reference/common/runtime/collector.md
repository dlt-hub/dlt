---
sidebar_label: collector
title: common.runtime.collector
---

## Collector Objects

```python
class Collector(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L35)

### update

```python
@abstractmethod
def update(name: str,
           inc: int = 1,
           total: int = None,
           inc_total: int = None,
           message: str = None,
           label: str = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L39)

Creates or updates a counter

This function updates a counter `name` with a value `inc`. If counter does not exist, it is created with optional total value of `total`.
Depending on implementation `label` may be used to create nested counters and message to display additional information associated with a counter.

**Arguments**:

- `name` _str_ - An unique name of a counter, displayable.
- `inc` _int, optional_ - Increase amount. Defaults to 1.
- `total` _int, optional_ - Maximum value of a counter. Defaults to None which means unbound counter.
- `icn_total` _int, optional_ - Increase the maximum value of the counter, does nothing if counter does not exit yet
- `message` _str, optional_ - Additional message attached to a counter. Defaults to None.
- `label` _str, optional_ - Creates nested counter for counter `name`. Defaults to None.

### \_\_call\_\_

```python
def __call__(step: str) -> TCollector
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L73)

Syntactic sugar for nicer context managers

## NullCollector Objects

```python
class NullCollector(Collector)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L86)

A default counter that does not count anything.

## DictCollector Objects

```python
class DictCollector(Collector)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L107)

A collector that just counts

## LogCollector Objects

```python
class LogCollector(Collector)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L132)

A Collector that shows progress by writing to a Python logger or a console

### \_\_init\_\_

```python
def __init__(log_period: float = 1.0,
             logger: Union[logging.Logger, TextIO] = sys.stdout,
             log_level: int = logging.INFO,
             dump_system_stats: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L143)

Collector writing to a `logger` every `log_period` seconds. The logger can be a Python logger instance, text stream, or None that will attach `dlt` logger

**Arguments**:

- `log_period` _float, optional_ - Time period in seconds between log updates. Defaults to 1.0.
- `logger` _logging.Logger | TextIO, optional_ - Logger or text stream to write log messages to. Defaults to stdio.
- `log_level` _str, optional_ - Log level for the logger. Defaults to INFO level
- `dump_system_stats` _bool, optional_ - Log memory and cpu usage. Defaults to True

## TqdmCollector Objects

```python
class TqdmCollector(Collector)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L283)

A Collector that shows progress with `tqdm` progress bars

### \_\_init\_\_

```python
def __init__(single_bar: bool = False, **tqdm_kwargs: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L286)

A Collector that uses tqdm to display counters as progress bars. Set `single_bar` to True to show just the main progress bar. Pass any config to tqdm in kwargs

## AliveCollector Objects

```python
class AliveCollector(Collector)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L341)

A Collector that shows progress with `alive-progress` progress bars

### \_\_init\_\_

```python
def __init__(single_bar: bool = True, **alive_kwargs: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L344)

Collector that uses alive_progress to display counters as progress bars. Set `single_bar` to True to show just the main progress bar. Pass any config to alive_progress in kwargs

## EnlightenCollector Objects

```python
class EnlightenCollector(Collector)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L410)

A Collector that shows progress with `enlighten` progress and status bars that also allow for logging.

### \_\_init\_\_

```python
def __init__(single_bar: bool = False, **enlighten_kwargs: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/collector.py#L417)

Collector that uses Enlighten to display counters as progress bars. Set `single_bar` to True to show just the main progress bar. Pass any config to Enlighten in kwargs

