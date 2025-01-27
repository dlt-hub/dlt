---
sidebar_label: runnable
title: common.runners.runnable
---

## Runnable Objects

```python
class Runnable(ABC, Generic[TExecutor])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/runners/runnable.py#L14)

### \_\_new\_\_

```python
def __new__(cls: Type["Runnable[TExecutor]"], *args: Any,
            **kwargs: Any) -> "Runnable[TExecutor]"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/runners/runnable.py#L23)

Registers Runnable instance as running for a time when context is active.
Used with `~workermethod` decorator to pass a class instance to decorator function that must be static thus avoiding pickling such instance.

**Arguments**:

- `cls` _Type[&quot;Runnable&quot;]_ - type of class to be instantiated
  

**Returns**:

- `Runnable` - new class instance

## workermethod

```python
def workermethod(f: TFun) -> TFun
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/runners/runnable.py#L44)

Decorator to be used on static method of Runnable to make it behave like instance method.
Expects that first parameter to decorated function is an instance `id` of Runnable that gets translated into Runnable instance.
Such instance is then passed as `self` to decorated function.

**Arguments**:

- `f` _TFun_ - worker function to be decorated
  

**Returns**:

- `TFun` - wrapped worker function

