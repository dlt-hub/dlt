---
sidebar_label: resource
title: extract.resource
---

## with\_table\_name

```python
def with_table_name(item: TDataItems, table_name: str) -> DataItemWithMeta
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L63)

Marks `item` to be dispatched to table `table_name` when yielded from resource function.

## with\_hints

```python
def with_hints(item: TDataItems,
               hints: TResourceHints,
               create_table_variant: bool = False) -> DataItemWithMeta
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L68)

Marks `item` to update the resource with specified `hints`.

Will create a separate variant of hints for a table if `name` is provided in `hints` and `create_table_variant` is set.

Create `TResourceHints` with `make_hints`.
Setting `table_name` will dispatch the `item` to a specified table, like `with_table_name`

## DltResource Objects

```python
class DltResource(Iterable[TDataItem], DltResourceHints)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L84)

Implements dlt resource. Contains a data pipe that wraps a generating item and table schema that can be adjusted

### source\_name

Name of the source that contains this instance of the source, set when added to DltResourcesDict

### section

A config section name

### from\_data

```python
@classmethod
def from_data(cls,
              data: Any,
              name: str = None,
              section: str = None,
              hints: TResourceHints = None,
              selected: bool = True,
              data_from: Union["DltResource", Pipe] = None,
              inject_config: bool = False) -> Self
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L110)

Creates an instance of DltResource from compatible `data` with a given `name` and `section`.

Internally (in the most common case) a new instance of Pipe with `name` is created from `data` and
optionally connected to an existing pipe `from_data` to form a transformer (dependent resource).

If `inject_config` is set to True and data is a callable, the callable is wrapped in incremental and config
injection wrappers.

### name

```python
@property
def name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L184)

Resource name inherited from the pipe

### with\_name

```python
def with_name(new_name: str) -> TDltResourceImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L188)

Clones the resource with a new name. Such resource keeps separate state and loads data to `new_name` table by default.

### is\_transformer

```python
@property
def is_transformer() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L193)

Checks if the resource is a transformer that takes data from another resource

### requires\_args

```python
@property
def requires_args() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L198)

Checks if resource has unbound arguments

### incremental

```python
@property
def incremental() -> IncrementalResourceWrapper
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L207)

Gets incremental transform if it is in the pipe

### validator

```python
@property
def validator() -> Optional[ValidateItem]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L216)

Gets validator transform if it is in the pipe

### validator

```python
@validator.setter
def validator(validator: Optional[ValidateItem]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L225)

Add/remove or replace the validator in pipe

### max\_table\_nesting

```python
@property
def max_table_nesting() -> Optional[int]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L234)

A schema hint for resource that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.

### pipe\_data\_from

```python
def pipe_data_from(data_from: Union[TDltResourceImpl, Pipe]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L247)

Replaces the parent in the transformer resource pipe from which the data is piped.

### add\_pipe

```python
def add_pipe(data: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L258)

Creates additional pipe for the resource from the specified data

### select\_tables

```python
def select_tables(*table_names: Iterable[str]) -> TDltResourceImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L263)

For resources that dynamically dispatch data to several tables allows to select tables that will receive data, effectively filtering out other data items.

Both `with_table_name` marker and data-based (function) table name hints are supported.

### add\_map

```python
def add_map(item_map: ItemTransformFunc[TDataItem],
            insert_at: int = None) -> TDltResourceImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L278)

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
                  insert_at: int = None) -> TDltResourceImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L298)

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
               insert_at: int = None) -> TDltResourceImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L321)

Adds filter defined in `item_filter` to the resource pipe at position `inserted_at`

`item_filter` receives single data items, `dlt` will enumerate any lists of data items automatically

**Arguments**:

- `item_filter` _ItemTransformFunc[bool]_ - A function taking a single data item and optional meta argument. Returns bool. If True, item is kept
- `insert_at` _int, optional_ - At which step in pipe to insert the filter. Defaults to None which inserts after last step

**Returns**:

- `"DltResource"` - returns self

### add\_limit

```python
def add_limit(max_items: int) -> TDltResourceImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L340)

Adds a limit `max_items` to the resource pipe.

This mutates the encapsulated generator to stop after `max_items` items are yielded. This is useful for testing and debugging.

**Notes**:

  1. Transformers won't be limited. They should process all the data they receive fully to avoid inconsistencies in generated datasets.
  2. Each yielded item may contain several records. `add_limit` only limits the "number of yields", not the total number of records.
  3. Async resources with a limit added may occasionally produce one item more than the limit on some runs. This behavior is not deterministic.
  

**Arguments**:

- `max_items` _int_ - The maximum number of items to yield

**Returns**:

- `"DltResource"` - returns self

### parallelize

```python
def parallelize() -> TDltResourceImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L408)

Wraps the resource to execute each item in a threadpool to allow multiple resources to extract in parallel.

The resource must be a generator or generator function or a transformer function.

### bind

```python
def bind(*args: Any, **kwargs: Any) -> TDltResourceImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L475)

Binds the parametrized resource to passed arguments. Modifies resource pipe in place. Does not evaluate generators or iterators.

### args\_bound

```python
@property
def args_bound() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L502)

Returns true if resource the parameters are bound to values. Such resource cannot be further called.
Note that resources are lazily evaluated and arguments are only formally checked. Configuration
was not yet injected as well.

### explicit\_args

```python
@property
def explicit_args() -> StrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L510)

Returns a dictionary of arguments used to parametrize the resource. Does not include defaults and injected args.

### state

```python
@property
def state() -> StrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L517)

Gets resource-scoped state from the active pipeline. PipelineStateNotAvailable is raised if pipeline context is not available

### \_\_call\_\_

```python
def __call__(*args: Any, **kwargs: Any) -> TDltResourceImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L522)

Binds the parametrized resources to passed arguments. Creates and returns a bound resource. Generators and iterators are not evaluated.

### \_\_or\_\_

```python
def __or__(transform: Union["DltResource", AnyFun]) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L529)

Allows to pipe data from across resources and transform functions with | operator
This is the LEFT side OR so the self may be resource or transformer

### \_\_ror\_\_

```python
def __ror__(data: Union[Iterable[Any], Iterator[Any]]) -> TDltResourceImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L545)

Allows to pipe data from across resources and transform functions with | operator
This is the RIGHT side OR so the self may not be a resource and the LEFT must be an object
that does not implement | ie. a list

### \_\_iter\_\_

```python
def __iter__() -> Iterator[TDataItem]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/resource.py#L555)

Opens iterator that yields the data items from the resources in the same order as in Pipeline class.

A read-only state is provided, initialized from active pipeline state. The state is discarded after the iterator is closed.

