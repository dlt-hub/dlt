---
sidebar_label: segment
title: common.runtime.segment
---

dltHub telemetry using Segment

#### track

```python
def track(event_category: TEventCategory, event_name: str,
          properties: DictStrAny) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/segment.py#L54)

Tracks a telemetry event.

The segment event name will be created as "{event_category}_{event_name}

**Arguments**:

- `event_category` - Category of the event: pipeline or cli
- `event_name` - Name of the event.
- `properties` - Dictionary containing the event's properties.

#### before\_send

```python
def before_send(event: DictStrAny) -> Optional[DictStrAny]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/segment.py#L83)

Called before sending event. Does nothing, patch this function in the module for custom behavior

#### get\_anonymous\_id

```python
def get_anonymous_id() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/segment.py#L114)

Creates or reads a anonymous user id

