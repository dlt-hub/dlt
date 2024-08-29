---
sidebar_label: configuration
title: destinations.impl.weaviate.configuration
---

## WeaviateCredentials Objects

```python
@configspec
class WeaviateCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/weaviate/configuration.py#L14)

### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/weaviate/configuration.py#L19)

Used to display user friendly data location

## WeaviateClientConfiguration Objects

```python
@configspec
class WeaviateClientConfiguration(DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/weaviate/configuration.py#L26)

### destination\_type

type: ignore

### dataset\_name

type: ignore[misc]

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/weaviate/configuration.py#L54)

Returns a fingerprint of host part of a connection string

