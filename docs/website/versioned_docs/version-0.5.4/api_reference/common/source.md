---
sidebar_label: source
title: common.source
---

## SourceInfo Objects

```python
class SourceInfo(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/source.py#L11)

Runtime information on the source/resource

## set\_current\_pipe\_name

```python
def set_current_pipe_name(name: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/source.py#L26)

Set pipe name in current thread

## unset\_current\_pipe\_name

```python
def unset_current_pipe_name() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/source.py#L31)

Unset pipe name in current thread

## get\_current\_pipe\_name

```python
def get_current_pipe_name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/source.py#L36)

When executed from withing dlt.resource decorated function, gets pipe name associated with current thread.

Pipe name is the same as resource name for all currently known cases. In some multithreading cases, pipe name may be not available.

