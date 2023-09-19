---
sidebar_label: typing
title: extract.typing
---

## SupportsPipe Objects

```python
class SupportsPipe(Protocol)
```

A protocol with the core Pipe properties and operations

#### name

Pipe name which is inherited by a resource

## ItemTransform Objects

```python
class ItemTransform(ABC, Generic[TAny])
```

#### \_\_call\_\_

```python
@abstractmethod
def __call__(item: TDataItems, meta: Any = None) -> Optional[TDataItems]
```

Transforms `item` (a list of TDataItem or a single TDataItem) and returns or yields TDataItems. Returns None to consume item (filter out)

