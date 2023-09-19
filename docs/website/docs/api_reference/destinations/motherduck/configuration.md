---
sidebar_label: configuration
title: destinations.motherduck.configuration
---

## MotherDuckCredentials Objects

```python
@configspec
class MotherDuckCredentials(DuckDbBaseCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/motherduck/configuration.py#L15)

#### drivername

type: ignore

#### read\_only

open database read/write

## MotherDuckClientConfiguration Objects

```python
@configspec
class MotherDuckClientConfiguration(
        DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/motherduck/configuration.py#L42)

#### destination\_name

type: ignore

#### create\_indexes

should unique indexes be created, this slows loading down massively

#### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/motherduck/configuration.py#L48)

Returns a fingerprint of user access token

