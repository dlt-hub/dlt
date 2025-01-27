---
sidebar_label: configuration
title: destinations.impl.weaviate.configuration
---

## WeaviateCredentials Objects

```python
@configspec
class WeaviateCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/weaviate/configuration.py#L15)

### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/weaviate/configuration.py#L20)

Used to display user friendly data location

## WeaviateClientConfiguration Objects

```python
@configspec
class WeaviateClientConfiguration(DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/weaviate/configuration.py#L27)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/weaviate/configuration.py#L57)

Returns a fingerprint of host part of a connection string

