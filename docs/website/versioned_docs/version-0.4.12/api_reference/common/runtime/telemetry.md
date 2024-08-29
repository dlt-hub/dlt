---
sidebar_label: telemetry
title: common.runtime.telemetry
---

## with\_telemetry

```python
def with_telemetry(category: TEventCategory, command: str, track_before: bool,
                   *args: str) -> Callable[[TFun], TFun]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/runtime/telemetry.py#L73)

Adds telemetry to f: TFun and add optional f *args values to `properties` of telemetry event

