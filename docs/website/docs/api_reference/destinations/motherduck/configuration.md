---
sidebar_label: configuration
title: destinations.motherduck.configuration
---

## MotherDuckCredentials Objects

```python
@configspec
class MotherDuckCredentials(DuckDbBaseCredentials)
```

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

#### destination\_name

type: ignore

#### create\_indexes

should unique indexes be created, this slows loading down massively

#### fingerprint

```python
def fingerprint() -> str
```

Returns a fingerprint of user access token

