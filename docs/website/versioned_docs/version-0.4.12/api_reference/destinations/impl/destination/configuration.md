---
sidebar_label: configuration
title: destinations.impl.destination.configuration
---

## CustomDestinationClientConfiguration Objects

```python
@configspec
class CustomDestinationClientConfiguration(DestinationClientConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/destination/configuration.py#L22)

### destination\_type

type: ignore

### destination\_callable

noqa: A003

### ensure\_callable

```python
def ensure_callable() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/destination/configuration.py#L30)

Makes sure that valid callable was provided

