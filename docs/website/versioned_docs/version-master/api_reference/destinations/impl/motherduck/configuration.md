---
sidebar_label: configuration
title: destinations.impl.motherduck.configuration
---

## MotherDuckCredentials Objects

```python
@configspec(init=False)
class MotherDuckCredentials(DuckDbBaseCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/motherduck/configuration.py#L21)

### read\_only

open database read/write

### on\_partial

```python
def on_partial() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/motherduck/configuration.py#L70)

Takes a token from query string and reuses it as a password

## MotherDuckClientConfiguration Objects

```python
@configspec
class MotherDuckClientConfiguration(
        DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/motherduck/configuration.py#L94)

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/motherduck/configuration.py#L104)

Returns a fingerprint of user access token

