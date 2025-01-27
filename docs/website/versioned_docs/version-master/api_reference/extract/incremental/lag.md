---
sidebar_label: lag
title: extract.incremental.lag
---

## apply\_lag

```python
def apply_lag(lag: float, initial_value: TCursorValue,
              last_value: TCursorValue,
              last_value_func: LastValueFunc[TCursorValue]) -> TCursorValue
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/incremental/lag.py#L59)

Applies lag to `last_value` but prevents it to cross `initial_value`: observing order of last_value_func

