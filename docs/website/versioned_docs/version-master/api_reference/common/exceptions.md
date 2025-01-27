---
sidebar_label: exceptions
title: common.exceptions
---

## ExceptionTrace Objects

```python
class ExceptionTrace(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/exceptions.py#L4)

Exception trace. NOTE: we intend to change it with an extended line by line trace with code snippets

### is\_terminal

Says if exception is terminal if happened to a job during load step

### exception\_attrs

Public attributes of an exception deriving from DltException (not starting with _)

### load\_id

Load id if found in exception attributes

### pipeline\_name

Pipeline name if found in exception attributes or in the active pipeline (Container)

### source\_name

Source name if found in exception attributes or in Container

### resource\_name

Resource name if found in exception attributes

### job\_id

Job id if found in exception attributes

## DltException Objects

```python
class DltException(Exception)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/exceptions.py#L27)

### \_\_reduce\_\_

```python
def __reduce__() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/exceptions.py#L28)

Enables exceptions with parametrized constructor to be pickled

### attrs

```python
def attrs() -> Dict[str, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/exceptions.py#L32)

Returns "public" attributes of the DltException

## TerminalException Objects

```python
class TerminalException(BaseException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/exceptions.py#L70)

Marks an exception that cannot be recovered from, should be mixed in into concrete exception class

## TransientException Objects

```python
class TransientException(BaseException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/exceptions.py#L76)

Marks an exception in operation that can be retried, should be mixed in into concrete exception class

## TerminalValueError Objects

```python
class TerminalValueError(ValueError, TerminalException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/exceptions.py#L82)

ValueError that is unrecoverable

## SignalReceivedException Objects

```python
class SignalReceivedException(KeyboardInterrupt, TerminalException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/exceptions.py#L88)

Raises when signal comes. Derives from `BaseException` to not be caught in regular exception handlers.

## PipelineException Objects

```python
class PipelineException(DltException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/exceptions.py#L169)

### \_\_init\_\_

```python
def __init__(pipeline_name: str, msg: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/exceptions.py#L170)

Base class for all pipeline exceptions. Should not be raised.

