---
sidebar_label: utils
title: extract.utils
---

## get\_data\_item\_format

```python
def get_data_item_format(items: TDataItems) -> TDataItemFormat
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/utils.py#L64)

Detect the format of the data item from `items`.

Reverts to `object` for empty lists

**Returns**:

  The data file format.

## resolve\_column\_value

```python
def resolve_column_value(column_hint: TTableHintTemplate[TColumnNames],
                         item: TDataItem) -> Union[Any, List[Any]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/utils.py#L88)

Extract values from the data item given a column hint.
Returns either a single value or list of values when hint is a composite.

## ensure\_table\_schema\_columns

```python
def ensure_table_schema_columns(
        columns: TAnySchemaColumns) -> TTableSchemaColumns
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/utils.py#L100)

Convert supported column schema types to a column dict which
can be used in resource schema.

**Arguments**:

- `columns` - A dict of column schemas, a list of column schemas, or a pydantic model

## ensure\_table\_schema\_columns\_hint

```python
def ensure_table_schema_columns_hint(
    columns: TTableHintTemplate[TAnySchemaColumns]
) -> TTableHintTemplate[TTableSchemaColumns]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/utils.py#L123)

Convert column schema hint to a hint returning `TTableSchemaColumns`.
A callable hint is wrapped in another function which converts the original result.

## reset\_pipe\_state

```python
def reset_pipe_state(pipe: SupportsPipe,
                     source_state_: Optional[DictStrAny] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/utils.py#L141)

Resets the resource state for a `pipe` and all its parent pipes

## simulate\_func\_call

```python
def simulate_func_call(
    f: Union[Any, AnyFun], args_to_skip: int, *args: Any, **kwargs: Any
) -> Tuple[inspect.Signature, inspect.Signature, inspect.BoundArguments]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/utils.py#L148)

Simulates a call to a resource or transformer function before it will be wrapped for later execution in the pipe

Returns a tuple with a `f` signature, modified signature in case of transformers and bound arguments

## wrap\_async\_iterator

```python
def wrap_async_iterator(
    gen: AsyncIterator[TDataItems]
) -> Generator[Awaitable[TDataItems], None, None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/utils.py#L186)

Wraps an async generator into a list of awaitables

## wrap\_parallel\_iterator

```python
def wrap_parallel_iterator(f: TAnyFunOrGenerator) -> TAnyFunOrGenerator
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/utils.py#L225)

Wraps a generator for parallel extraction

## wrap\_compat\_transformer

```python
def wrap_compat_transformer(name: str, f: AnyFun, sig: inspect.Signature,
                            *args: Any, **kwargs: Any) -> AnyFun
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/utils.py#L274)

Creates a compatible wrapper over transformer function. A pure transformer function expects data item in first argument and one keyword argument called `meta`

## wrap\_resource\_gen

```python
def wrap_resource_gen(name: str, f: AnyFun, sig: inspect.Signature, *args: Any,
                      **kwargs: Any) -> AnyFun
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/utils.py#L293)

Wraps a generator or generator function so it is evaluated on extraction

