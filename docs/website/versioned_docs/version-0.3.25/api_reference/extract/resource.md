---
sidebar_label: resource
title: extract.resource
---

## with\_table\_name

```python
def with_table_name(item: TDataItems, table_name: str) -> DataItemWithMeta
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L61)

Marks `item` to be dispatched to table `table_name` when yielded from resource function.

## with\_hints

```python
def with_hints(item: TDataItems, hints: TResourceHints) -> DataItemWithMeta
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L66)

Marks `item` to update the resource with specified `hints`.

Create `TResourceHints` with `make_hints`.
Setting `table_name` will dispatch the `item` to a specified table, like `with_table_name`

## DltResource Objects

```python
class DltResource(Iterable[TDataItem], DltResourceHints)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L75)

Implements dlt resource. Contains a data pipe that wraps a generating item and table schema that can be adjusted

### source\_name

Name of the source that contains this instance of the source, set when added to DltResourcesDict

### section

A config section name

### name

```python
@property
def name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L165)

Resource name inherited from the pipe

### with\_name

```python
def with_name(new_name: str) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L169)

Clones the resource with a new name. Such resource keeps separate state and loads data to `new_name` table by default.

### is\_transformer

```python
@property
def is_transformer() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L174)

Checks if the resource is a transformer that takes data from another resource

### requires\_args

```python
@property
def requires_args() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L179)

Checks if resource has unbound arguments

### incremental

```python
@property
def incremental() -> IncrementalResourceWrapper
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L188)

Gets incremental transform if it is in the pipe

### validator

```python
@property
def validator() -> Optional[ValidateItem]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L197)

Gets validator transform if it is in the pipe

### validator

```python
@validator.setter
def validator(validator: Optional[ValidateItem]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L206)

Add/remove or replace the validator in pipe

### pipe\_data\_from

```python
def pipe_data_from(data_from: Union["DltResource", Pipe]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L214)

Replaces the parent in the transformer resource pipe from which the data is piped.

### add\_pipe

```python
def add_pipe(data: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L225)

Creates additional pipe for the resource from the specified data

### select\_tables

```python
def select_tables(*table_names: Iterable[str]) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L230)

For resources that dynamically dispatch data to several tables allows to select tables that will receive data, effectively filtering out other data items.

Both `with_table_name` marker and data-based (function) table name hints are supported.

### add\_map

```python
def add_map(item_map: ItemTransformFunc[TDataItem],
            insert_at: int = None) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L245)

Adds mapping function defined in `item_map` to the resource pipe at position `inserted_at`

`item_map` receives single data items, `dlt` will enumerate any lists of data items automatically

**Arguments**:

- `item_map` _ItemTransformFunc[TDataItem]_ - A function taking a single data item and optional meta argument. Returns transformed data item.
- `insert_at` _int, optional_ - At which step in pipe to insert the mapping. Defaults to None which inserts after last step
  

**Returns**:

- `"DltResource"` - returns self

### add\_yield\_map

```python
def add_yield_map(item_map: ItemTransformFunc[Iterator[TDataItem]],
                  insert_at: int = None) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L265)

Adds generating function defined in `item_map` to the resource pipe at position `inserted_at`

`item_map` receives single data items, `dlt` will enumerate any lists of data items automatically. It may yield 0 or more data items and be used to
ie. pivot an item into sequence of rows.

**Arguments**:

- `item_map` _ItemTransformFunc[Iterator[TDataItem]]_ - A function taking a single data item and optional meta argument. Yields 0 or more data items.
- `insert_at` _int, optional_ - At which step in pipe to insert the generator. Defaults to None which inserts after last step
  

**Returns**:

- `"DltResource"` - returns self

### add\_filter

```python
def add_filter(item_filter: ItemTransformFunc[bool],
               insert_at: int = None) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L286)

Adds filter defined in `item_filter` to the resource pipe at position `inserted_at`

`item_filter` receives single data items, `dlt` will enumerate any lists of data items automatically

**Arguments**:

- `item_filter` _ItemTransformFunc[bool]_ - A function taking a single data item and optional meta argument. Returns bool. If True, item is kept
- `insert_at` _int, optional_ - At which step in pipe to insert the filter. Defaults to None which inserts after last step

**Returns**:

- `"DltResource"` - returns self

### add\_limit

```python
def add_limit(max_items: int) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L305)

Adds a limit `max_items` to the resource pipe

This mutates the encapsulated generator to stop after `max_items` items are yielded. This is useful for testing and debugging. It is
a no-op for transformers. Those should be limited by their input data.

**Arguments**:

- `max_items` _int_ - The maximum number of items to yield

**Returns**:

- `"DltResource"` - returns self

### parallelize

```python
def parallelize() -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L364)

Wraps the resource to execute each item in a threadpool to allow multiple resources to extract in parallel.

The resource must be a generator or generator function or a transformer function.

### bind

```python
def bind(*args: Any, **kwargs: Any) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L411)

Binds the parametrized resource to passed arguments. Modifies resource pipe in place. Does not evaluate generators or iterators.

### explicit\_args

```python
@property
def explicit_args() -> StrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L438)

Returns a dictionary of arguments used to parametrize the resource. Does not include defaults and injected args.

### state

```python
@property
def state() -> StrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L445)

Gets resource-scoped state from the active pipeline. PipelineStateNotAvailable is raised if pipeline context is not available

### \_\_call\_\_

```python
def __call__(*args: Any, **kwargs: Any) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L450)

Binds the parametrized resources to passed arguments. Creates and returns a bound resource. Generators and iterators are not evaluated.

### \_\_or\_\_

```python
def __or__(transform: Union["DltResource", AnyFun]) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L457)

Allows to pipe data from across resources and transform functions with | operator
This is the LEFT side OR so the self may be resource or transformer

### \_\_ror\_\_

```python
def __ror__(data: Union[Iterable[Any], Iterator[Any]]) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L473)

Allows to pipe data from across resources and transform functions with | operator
This is the RIGHT side OR so the self may not be a resource and the LEFT must be an object
that does not implement | ie. a list

### \_\_iter\_\_

```python
def __iter__() -> Iterator[TDataItem]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/resource.py#L481)

Opens iterator that yields the data items from the resources in the same order as in Pipeline class.

A read-only state is provided, initialized from active pipeline state. The state is discarded after the iterator is closed.

