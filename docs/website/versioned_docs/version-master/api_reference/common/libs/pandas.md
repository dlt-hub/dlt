---
sidebar_label: pandas
title: common.libs.pandas
---

## pandas\_to\_arrow

```python
def pandas_to_arrow(df: pandas.DataFrame, preserve_index: bool = False) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/pandas.py#L11)

Converts pandas to arrow or raises an exception if pyarrow is not installed

