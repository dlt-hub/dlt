---
sidebar_label: configuration
title: destinations.impl.destination.configuration
---

## CustomDestinationClientConfiguration Objects

```python
@configspec
class CustomDestinationClientConfiguration(DestinationClientConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/destination/configuration.py#L22)

### destination\_type

type: ignore

### destination\_callable

noqa: A003

### ensure\_callable

```python
def ensure_callable() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/destination/configuration.py#L30)

Makes sure that valid callable was provided

