---
sidebar_label: configuration
title: destinations.redshift.configuration
---

## RedshiftClientConfiguration Objects

```python
@configspec
class RedshiftClientConfiguration(PostgresClientConfiguration)
```

#### destination\_name

type: ignore

#### fingerprint

```python
def fingerprint() -> str
```

Returns a fingerprint of host part of a connection string

