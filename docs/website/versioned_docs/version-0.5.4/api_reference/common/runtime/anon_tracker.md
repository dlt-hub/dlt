---
sidebar_label: anon_tracker
title: common.runtime.anon_tracker
---

dltHub telemetry using using anonymous tracker

## track

```python
def track(event_category: TEventCategory, event_name: str,
          properties: DictStrAny) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/runtime/anon_tracker.py#L72)

Tracks a telemetry event.

The tracker event name will be created as "{event_category}_{event_name}

**Arguments**:

- `event_category` - Category of the event: pipeline or cli
- `event_name` - Name of the event.
- `properties` - Dictionary containing the event's properties.

## before\_send

```python
def before_send(event: DictStrAny) -> Optional[DictStrAny]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/runtime/anon_tracker.py#L94)

Called before sending event. Does nothing, patch this function in the module for custom behavior

## get\_anonymous\_id

```python
def get_anonymous_id() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/runtime/anon_tracker.py#L114)

Creates or reads a anonymous user id

