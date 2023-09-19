---
sidebar_label: typing
title: extract.typing
---

## SupportsPipe Objects

```python
class SupportsPipe(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/typing.py#L38)

A protocol with the core Pipe properties and operations

#### name

Pipe name which is inherited by a resource

## ItemTransform Objects

```python
class ItemTransform(ABC, Generic[TAny])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/typing.py#L48)

#### \_\_call\_\_

```python
@abstractmethod
def __call__(item: TDataItems, meta: Any = None) -> Optional[TDataItems]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/typing.py#L65)

Transforms `item` (a list of TDataItem or a single TDataItem) and returns or yields TDataItems. Returns None to consume item (filter out)

