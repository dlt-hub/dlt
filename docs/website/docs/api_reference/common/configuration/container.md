---
sidebar_label: container
title: common.configuration.container
---

## Container Objects

```python
class Container()
```

A singleton injection container holding several injection contexts. Implements basic dictionary interface.

Injection context is identified by its type and available via dict indexer. The common pattern is to instantiate default context value
if it is not yet present in container.

The indexer is settable and allows to explicitly set the value. This is required by for context that needs to be explicitly instantiated.

The `injectable_context` allows to set a context with a `with` keyword and then restore the previous one after it gets out of scope.

#### injectable\_context

```python
@contextmanager
def injectable_context(config: TConfiguration) -> Iterator[TConfiguration]
```

A context manager that will insert `config` into the container and restore the previous value when it gets out of scope.

