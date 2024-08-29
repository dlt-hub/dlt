---
sidebar_label: configuration
title: destinations.impl.redshift.configuration
---

## RedshiftClientConfiguration Objects

```python
@configspec
class RedshiftClientConfiguration(PostgresClientConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/redshift/configuration.py#L23)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/redshift/configuration.py#L30)

Returns a fingerprint of host part of a connection string

