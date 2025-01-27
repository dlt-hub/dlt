---
sidebar_label: telemetry
title: common.runtime.telemetry
---

## with\_telemetry

```python
def with_telemetry(category: TEventCategory, command: str, track_before: bool,
                   *args: str) -> Callable[[TFun], TFun]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/telemetry.py#L73)

Adds telemetry to f: TFun and add optional f *args values to `properties` of telemetry event

