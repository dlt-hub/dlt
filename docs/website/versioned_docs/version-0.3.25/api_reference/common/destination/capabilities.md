---
sidebar_label: capabilities
title: common.destination.capabilities
---

## DestinationCapabilitiesContext Objects

```python
@configspec
class DestinationCapabilitiesContext(ContainerInjectableContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/capabilities.py#L30)

Injectable destination capabilities required for many Pipeline stages ie. normalize

### max\_table\_nesting

Destination supports CREATE TABLE ... CLONE ... statements

