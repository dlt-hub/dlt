---
sidebar_label: configuration
title: destinations.impl.dremio.configuration
---

## DremioClientConfiguration Objects

```python
@configspec
class DremioClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/dremio/configuration.py#L33)

### destination\_type

type: ignore[misc]

### staging\_data\_source

The name of the staging data source

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/dremio/configuration.py#L39)

Returns a fingerprint of host part of a connection string

