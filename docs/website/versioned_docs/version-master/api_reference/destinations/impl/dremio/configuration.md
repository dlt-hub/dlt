---
sidebar_label: configuration
title: destinations.impl.dremio.configuration
---

## DremioClientConfiguration Objects

```python
@configspec
class DremioClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/dremio/configuration.py#L34)

### destination\_type

type: ignore[misc]

### staging\_data\_source

The name of the staging data source

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/dremio/configuration.py#L40)

Returns a fingerprint of host part of a connection string

