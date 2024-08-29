---
sidebar_label: factory
title: destinations.impl.weaviate.factory
---

## weaviate Objects

```python
class weaviate(Destination[WeaviateClientConfiguration, "WeaviateClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/weaviate/factory.py#L15)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[WeaviateCredentials, t.Dict[str,
                                                              t.Any]] = None,
             vectorizer: str = None,
             module_config: t.Dict[str, t.Dict[str, str]] = None,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/weaviate/factory.py#L27)

Configure the Weaviate destination to use in a pipeline.

All destination config parameters can be provided as arguments here and will supersede other config sources (such as dlt config files and environment variables).

**Arguments**:

- `credentials` - Weaviate credentials containing URL, API key and optional headers
- `vectorizer` - The name of the Weaviate vectorizer to use
- `module_config` - The configuration for the Weaviate modules
- `**kwargs` - Additional arguments forwarded to the destination config

