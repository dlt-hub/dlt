---
sidebar_label: configuration
title: destinations.impl.redshift.configuration
---

## RedshiftClientConfiguration Objects

```python
@configspec
class RedshiftClientConfiguration(PostgresClientConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/redshift/configuration.py#L23)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/redshift/configuration.py#L30)

Returns a fingerprint of host part of a connection string

