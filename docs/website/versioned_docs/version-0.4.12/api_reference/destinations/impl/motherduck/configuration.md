---
sidebar_label: configuration
title: destinations.impl.motherduck.configuration
---

## MotherDuckCredentials Objects

```python
@configspec(init=False)
class MotherDuckCredentials(DuckDbBaseCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/motherduck/configuration.py#L19)

### read\_only

open database read/write

### on\_partial

```python
def on_partial() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/motherduck/configuration.py#L57)

Takes a token from query string and reuses it as a password

## MotherDuckClientConfiguration Objects

```python
@configspec
class MotherDuckClientConfiguration(
        DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/motherduck/configuration.py#L77)

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/motherduck/configuration.py#L87)

Returns a fingerprint of user access token

