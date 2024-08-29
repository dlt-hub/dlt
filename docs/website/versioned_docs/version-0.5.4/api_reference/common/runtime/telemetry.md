---
sidebar_label: telemetry
title: common.runtime.telemetry
---

## with\_telemetry

```python
def with_telemetry(category: TEventCategory, command: str, track_before: bool,
                   *args: str) -> Callable[[TFun], TFun]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/runtime/telemetry.py#L54)

Adds telemetry to f: TFun and add optional f *args values to `properties` of telemetry event

