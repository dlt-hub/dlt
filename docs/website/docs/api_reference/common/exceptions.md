---
sidebar_label: exceptions
title: common.exceptions
---

## DltException Objects

```python
class DltException(Exception)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/exceptions.py#L4)

#### \_\_reduce\_\_

```python
def __reduce__() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/exceptions.py#L5)

Enables exceptions with parametrized constructor to be pickled

## TerminalException Objects

```python
class TerminalException(BaseException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/exceptions.py#L32)

Marks an exception that cannot be recovered from, should be mixed in into concrete exception class

## TransientException Objects

```python
class TransientException(BaseException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/exceptions.py#L38)

Marks an exception in operation that can be retried, should be mixed in into concrete exception class

## TerminalValueError Objects

```python
class TerminalValueError(ValueError, TerminalException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/exceptions.py#L44)

ValueError that is unrecoverable

## SignalReceivedException Objects

```python
class SignalReceivedException(KeyboardInterrupt, TerminalException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/exceptions.py#L50)

Raises when signal comes. Derives from `BaseException` to not be caught in regular exception handlers.

## PipelineException Objects

```python
class PipelineException(DltException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/exceptions.py#L180)

#### \_\_init\_\_

```python
def __init__(pipeline_name: str, msg: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/exceptions.py#L181)

Base class for all pipeline exceptions. Should not be raised.

