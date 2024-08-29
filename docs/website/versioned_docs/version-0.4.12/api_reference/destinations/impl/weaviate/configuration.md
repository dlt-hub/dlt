---
sidebar_label: configuration
title: destinations.impl.weaviate.configuration
---

## WeaviateCredentials Objects

```python
@configspec
class WeaviateCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/weaviate/configuration.py#L15)

### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/weaviate/configuration.py#L20)

Used to display user friendly data location

## WeaviateClientConfiguration Objects

```python
@configspec
class WeaviateClientConfiguration(DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/weaviate/configuration.py#L27)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/weaviate/configuration.py#L57)

Returns a fingerprint of host part of a connection string

