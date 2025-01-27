---
sidebar_label: items
title: extract.items
---

## SupportsPipe Objects

```python
class SupportsPipe(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/items.py#L103)

A protocol with the core Pipe properties and operations

### name

Pipe name which is inherited by a resource

### parent

A parent of the current pipe

### gen

```python
@property
def gen() -> TPipeStep
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/items.py#L112)

A data generating step

### \_\_getitem\_\_

```python
def __getitem__(i: int) -> TPipeStep
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/items.py#L116)

Get pipe step at index

### \_\_len\_\_

```python
def __len__() -> int
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/items.py#L120)

Length of a pipe

### has\_parent

```python
@property
def has_parent() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/items.py#L125)

Checks if pipe is connected to parent pipe from which it takes data items. Connected pipes are created from transformer resources

### close

```python
def close() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/items.py#L129)

Closes pipe generator

## ItemTransform Objects

```python
class ItemTransform(ABC, Generic[TAny])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/items.py#L139)

### placement\_affinity

Tell how strongly an item sticks to start (-1) or end (+1) of pipe.

### \_\_call\_\_

```python
@abstractmethod
def __call__(item: TDataItems, meta: Any = None) -> Optional[TDataItems]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/items.py#L159)

Transforms `item` (a list of TDataItem or a single TDataItem) and returns or yields TDataItems. Returns None to consume item (filter out)

## ValidateItem Objects

```python
class ValidateItem(ItemTransform[TDataItem])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/items.py#L227)

Base class for validators of data items.

Subclass should implement the `__call__` method to either return the data item(s) or raise `extract.exceptions.ValidationError`.
See `PydanticValidator` for possible implementation.

### placement\_affinity

stick to end but less than incremental

