---
sidebar_label: configuration
title: destinations.impl.qdrant.configuration
---

## QdrantCredentials Objects

```python
@configspec
class QdrantCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/qdrant/configuration.py#L18)

### close\_client

```python
def close_client(client: "QdrantClient") -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/qdrant/configuration.py#L64)

Close client if not external

## QdrantClientConfiguration Objects

```python
@configspec
class QdrantClientConfiguration(DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/qdrant/configuration.py#L100)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/qdrant/configuration.py#L137)

Returns a fingerprint of a connection string

