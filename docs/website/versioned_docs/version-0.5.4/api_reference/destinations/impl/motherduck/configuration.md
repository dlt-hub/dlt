---
sidebar_label: configuration
title: destinations.impl.motherduck.configuration
---

## MotherDuckCredentials Objects

```python
@configspec
class MotherDuckCredentials(DuckDbBaseCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/motherduck/configuration.py#L16)

### drivername

type: ignore

### read\_only

open database read/write

## MotherDuckClientConfiguration Objects

```python
@configspec
class MotherDuckClientConfiguration(
        DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/motherduck/configuration.py#L59)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/motherduck/configuration.py#L67)

Returns a fingerprint of user access token

