---
sidebar_label: configuration
title: destinations.impl.weaviate.configuration
---

## WeaviateCredentials Objects

```python
@configspec
class WeaviateCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/weaviate/configuration.py#L15)

### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/weaviate/configuration.py#L20)

Used to display user friendly data location

## WeaviateClientConfiguration Objects

```python
@configspec
class WeaviateClientConfiguration(DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/weaviate/configuration.py#L27)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/weaviate/configuration.py#L57)

Returns a fingerprint of host part of a connection string

