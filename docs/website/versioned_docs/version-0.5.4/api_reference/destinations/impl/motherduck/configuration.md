---
sidebar_label: configuration
title: destinations.impl.motherduck.configuration
---

## MotherDuckCredentials Objects

```python
@configspec(init=False)
class MotherDuckCredentials(DuckDbBaseCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/motherduck/configuration.py#L19)

### read\_only

open database read/write

### on\_partial

```python
def on_partial() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/motherduck/configuration.py#L57)

Takes a token from query string and reuses it as a password

## MotherDuckClientConfiguration Objects

```python
@configspec
class MotherDuckClientConfiguration(
        DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/motherduck/configuration.py#L77)

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/motherduck/configuration.py#L87)

Returns a fingerprint of user access token

