---
sidebar_label: source
title: common.source
---

## SourceInfo Objects

```python
class SourceInfo(NamedTuple)
```

Runtime information on the source/resource

#### set\_current\_pipe\_name

```python
def set_current_pipe_name(name: str) -> None
```

Set pipe name in current thread

#### unset\_current\_pipe\_name

```python
def unset_current_pipe_name() -> None
```

Unset pipe name in current thread

#### get\_current\_pipe\_name

```python
def get_current_pipe_name() -> str
```

Gets pipe name associated with current thread

