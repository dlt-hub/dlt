---
sidebar_label: json_logging
title: common.runtime.json_logging
---

## config\_root\_logger

```python
def config_root_logger() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/json_logging.py#L54)

You must call this if you are using root logger.
Make all root logger' handlers produce JSON format
& remove duplicate handlers for request instrumentation logging.
Please made sure that you call this after you called "logging.basicConfig() or logging.getLogger()

## init

```python
def init(custom_formatter: Type[logging.Formatter] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/json_logging.py#L65)

This is supposed to be called only one time.

If **custom_formatter** is passed, it will (in non-web context) use this formatter over the default.

## BaseJSONFormatter Objects

```python
class BaseJSONFormatter(logging.Formatter)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/json_logging.py#L88)

Base class for JSON formatters

## JSONLogFormatter Objects

```python
class JSONLogFormatter(BaseJSONFormatter)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/json_logging.py#L143)

Formatter for non-web application log

## update\_formatter\_for\_loggers

```python
def update_formatter_for_loggers(loggers_iter: List[Logger],
                                 formatter: Type[logging.Formatter]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/json_logging.py#L182)

**Arguments**:

- `formatter`: 
- `loggers_iter`: 

