---
sidebar_label: source
title: common.source
---

## SourceInfo Objects

```python
class SourceInfo(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/source.py#L11)

Runtime information on the source/resource

#### set\_current\_pipe\_name

```python
def set_current_pipe_name(name: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/source.py#L25)

Set pipe name in current thread

#### unset\_current\_pipe\_name

```python
def unset_current_pipe_name() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/source.py#L30)

Unset pipe name in current thread

#### get\_current\_pipe\_name

```python
def get_current_pipe_name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/source.py#L35)

Gets pipe name associated with current thread

