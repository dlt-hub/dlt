---
sidebar_label: telemetry
title: common.runtime.telemetry
---

#### with\_telemetry

```python
def with_telemetry(category: TEventCategory, command: str, track_before: bool,
                   *args: str) -> Callable[[TFun], TFun]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/telemetry.py#L52)

Adds telemetry to f: TFun and add optional f *args values to `properties` of telemetry event

