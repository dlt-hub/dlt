---
sidebar_label: container
title: common.configuration.container
---

## Container Objects

```python
class Container()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/container.py#L16)

A singleton injection container holding several injection contexts. Implements basic dictionary interface.

Injection context is identified by its type and available via dict indexer. The common pattern is to instantiate default context value
if it is not yet present in container.

By default, the context is thread-affine so it can be injected only n the thread that originally set it. This behavior may be changed
in particular context type (spec).

The indexer is settable and allows to explicitly set the value. This is required by in any context that needs to be explicitly instantiated.

The `injectable_context` allows to set a context with a `with` keyword and then restore the previous one after it gets out of scope.

### thread\_contexts

A thread aware mapping of injection context

### main\_context

Injection context for the main thread

### injectable\_context

```python
@contextmanager
def injectable_context(config: TConfiguration,
                       lock_context: bool = False) -> Iterator[TConfiguration]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/container.py#L135)

A context manager that will insert `config` into the container and restore the previous value when it gets out of scope.

### thread\_pool\_prefix

```python
@staticmethod
def thread_pool_prefix() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/container.py#L175)

Creates a container friendly pool prefix that contains starting thread id. Container implementation will automatically use it
for any thread-affine contexts instead of using id of the pool thread

