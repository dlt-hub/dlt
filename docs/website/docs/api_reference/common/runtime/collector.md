---
sidebar_label: collector
title: common.runtime.collector
---

## Collector Objects

```python
class Collector(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L22)

#### update

```python
@abstractmethod
def update(name: str,
           inc: int = 1,
           total: int = None,
           message: str = None,
           label: str = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L27)

Creates or updates a counter

This function updates a counter `name` with a value `inc`. If counter does not exist, it is created with optional total value of `total`.
Depending on implementation `label` may be used to create nested counters and message to display additional information associated with a counter.

**Arguments**:

- `name` _str_ - An unique name of a counter, displayable.
- `inc` _int, optional_ - Increase amount. Defaults to 1.
- `total` _int, optional_ - Maximum value of a counter. Defaults to None which means unbound counter.
- `message` _str, optional_ - Additional message attached to a counter. Defaults to None.
- `label` _str, optional_ - Creates nested counter for counter `name`. Defaults to None.

#### \_\_call\_\_

```python
def __call__(step: str) -> TCollector
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L52)

Syntactic sugar for nicer context managers

## NullCollector Objects

```python
class NullCollector(Collector)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L65)

A default counter that does not count anything.

## DictCollector Objects

```python
class DictCollector(Collector)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L78)

A collector that just counts

## LogCollector Objects

```python
class LogCollector(Collector)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L95)

A Collector that shows progress by writing to a Python logger or a console

#### \_\_init\_\_

```python
def __init__(log_period: float = 1.0,
             logger: Union[logging.Logger, TextIO] = sys.stdout,
             log_level: int = logging.INFO,
             dump_system_stats: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L106)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L212)

A Collector that shows progress with `tqdm` progress bars

#### \_\_init\_\_

```python
def __init__(single_bar: bool = False, **tqdm_kwargs: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L215)

A Collector that uses tqdm to display counters as progress bars. Set `single_bar` to True to show just the main progress bar. Pass any config to tqdm in kwargs

## AliveCollector Objects

```python
class AliveCollector(Collector)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L256)

A Collector that shows progress with `alive-progress` progress bars

#### \_\_init\_\_

```python
def __init__(single_bar: bool = True, **alive_kwargs: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L259)

Collector that uses alive_progress to display counters as progress bars. Set `single_bar` to True to show just the main progress bar. Pass any config to alive_progress in kwargs

## EnlightenCollector Objects

```python
class EnlightenCollector(Collector)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L303)

A Collector that shows progress with `enlighten` progress and status bars that also allow for logging.

#### \_\_init\_\_

```python
def __init__(single_bar: bool = False, **enlighten_kwargs: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/collector.py#L310)

Collector that uses Enlighten to display counters as progress bars. Set `single_bar` to True to show just the main progress bar. Pass any config to Enlighten in kwargs

