---
sidebar_label: factory
title: destinations.impl.destination.factory
---

## DestinationInfo Objects

```python
class DestinationInfo(t.NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/destination/factory.py#L26)

Runtime information on a discovered destination

## destination Objects

```python
class destination(Destination[CustomDestinationClientConfiguration,
                              "DestinationClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/destination/factory.py#L38)

### spec

```python
@property
def spec() -> t.Type[CustomDestinationClientConfiguration]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/destination/factory.py#L51)

A spec of destination configuration resolved from the sink function signature

