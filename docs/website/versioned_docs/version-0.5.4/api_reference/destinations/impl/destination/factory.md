---
sidebar_label: factory
title: destinations.impl.destination.factory
---

## DestinationInfo Objects

```python
class DestinationInfo(t.NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/destination/factory.py#L24)

Runtime information on a discovered destination

## destination Objects

```python
class destination(Destination[GenericDestinationClientConfiguration,
                              "DestinationClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/destination/factory.py#L36)

### spec

```python
@property
def spec() -> t.Type[GenericDestinationClientConfiguration]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/destination/factory.py#L45)

A spec of destination configuration resolved from the sink function signature

