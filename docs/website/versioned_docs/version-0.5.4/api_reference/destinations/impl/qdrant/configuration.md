---
sidebar_label: configuration
title: destinations.impl.qdrant.configuration
---

## QdrantClientConfiguration Objects

```python
@configspec
class QdrantClientConfiguration(DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/qdrant/configuration.py#L49)

### destination\_type

type: ignore

### dataset\_name

type: ignore[misc]

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/qdrant/configuration.py#L79)

Returns a fingerprint of a connection string

