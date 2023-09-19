---
sidebar_label: telemetry
title: common.runtime.telemetry
---

#### with\_telemetry

```python
def with_telemetry(category: TEventCategory, command: str, track_before: bool,
                   *args: str) -> Callable[[TFun], TFun]
```

Adds telemetry to f: TFun and add optional f *args values to `properties` of telemetry event

