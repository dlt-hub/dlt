---
sidebar_label: configuration
title: destinations.impl.redshift.configuration
---

## RedshiftClientConfiguration Objects

```python
@configspec
class RedshiftClientConfiguration(PostgresClientConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/redshift/configuration.py#L22)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/redshift/configuration.py#L27)

Returns a fingerprint of host part of a connection string

