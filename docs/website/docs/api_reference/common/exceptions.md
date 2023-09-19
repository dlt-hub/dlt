---
sidebar_label: exceptions
title: common.exceptions
---

## DltException Objects

```python
class DltException(Exception)
```

#### \_\_reduce\_\_

```python
def __reduce__() -> Any
```

Enables exceptions with parametrized constructor to be pickled

## TerminalException Objects

```python
class TerminalException(BaseException)
```

Marks an exception that cannot be recovered from, should be mixed in into concrete exception class

## TransientException Objects

```python
class TransientException(BaseException)
```

Marks an exception in operation that can be retried, should be mixed in into concrete exception class

## TerminalValueError Objects

```python
class TerminalValueError(ValueError, TerminalException)
```

ValueError that is unrecoverable

## SignalReceivedException Objects

```python
class SignalReceivedException(KeyboardInterrupt, TerminalException)
```

Raises when signal comes. Derives from `BaseException` to not be caught in regular exception handlers.

## PipelineException Objects

```python
class PipelineException(DltException)
```

#### \_\_init\_\_

```python
def __init__(pipeline_name: str, msg: str) -> None
```

Base class for all pipeline exceptions. Should not be raised.

