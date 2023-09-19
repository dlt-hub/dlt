---
sidebar_label: source
title: extract.source
---

#### with\_table\_name

```python
def with_table_name(item: TDataItems, table_name: str) -> DataItemWithMeta
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L30)

Marks `item` to be dispatched to table `table_name` when yielded from resource function.

## DltResource Objects

```python
class DltResource(Iterable[TDataItem], DltResourceSchema)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L35)

#### source\_name

Name of the source that contains this instance of the source, set when added to DltResourcesDict

#### name

```python
@property
def name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L111)

Resource name inherited from the pipe

#### is\_transformer

```python
@property
def is_transformer() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L116)

Checks if the resource is a transformer that takes data from another resource

#### requires\_binding

```python
@property
def requires_binding() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L121)

Checks if resource has unbound parameters

#### incremental

```python
@property
def incremental() -> IncrementalResourceWrapper
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L130)

Gets incremental transform if it is in the pipe

#### pipe\_data\_from

```python
def pipe_data_from(data_from: Union["DltResource", Pipe]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L138)

Replaces the parent in the transformer resource pipe from which the data is piped.

#### add\_pipe

```python
def add_pipe(data: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L147)

Creates additional pipe for the resource from the specified data

#### select\_tables

```python
def select_tables(*table_names: Iterable[str]) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L152)

For resources that dynamically dispatch data to several tables allows to select tables that will receive data, effectively filtering out other data items.

Both `with_table_name` marker and data-based (function) table name hints are supported.

#### add\_map

```python
def add_map(item_map: ItemTransformFunc[TDataItem],
            insert_at: int = None) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L166)

Adds mapping function defined in `item_map` to the resource pipe at position `inserted_at`

`item_map` receives single data items, `dlt` will enumerate any lists of data items automatically

**Arguments**:

- `item_map` _ItemTransformFunc[TDataItem]_ - A function taking a single data item and optional meta argument. Returns transformed data item.
- `insert_at` _int, optional_ - At which step in pipe to insert the mapping. Defaults to None which inserts after last step
  

**Returns**:

- `"DltResource"` - returns self

#### add\_yield\_map

```python
def add_yield_map(item_map: ItemTransformFunc[Iterator[TDataItem]],
                  insert_at: int = None) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L184)

Adds generating function defined in `item_map` to the resource pipe at position `inserted_at`

`item_map` receives single data items, `dlt` will enumerate any lists of data items automatically. It may yield 0 or more data items and be used to
ie. pivot an item into sequence of rows.

**Arguments**:

- `item_map` _ItemTransformFunc[Iterator[TDataItem]]_ - A function taking a single data item and optional meta argument. Yields 0 or more data items.
- `insert_at` _int, optional_ - At which step in pipe to insert the generator. Defaults to None which inserts after last step
  

**Returns**:

- `"DltResource"` - returns self

#### add\_filter

```python
def add_filter(item_filter: ItemTransformFunc[bool],
               insert_at: int = None) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L203)

Adds filter defined in `item_filter` to the resource pipe at position `inserted_at`

`item_filter` receives single data items, `dlt` will enumerate any lists of data items automatically

**Arguments**:

- `item_filter` _ItemTransformFunc[bool]_ - A function taking a single data item and optional meta argument. Returns bool. If True, item is kept
- `insert_at` _int, optional_ - At which step in pipe to insert the filter. Defaults to None which inserts after last step

**Returns**:

- `"DltResource"` - returns self

#### add\_limit

```python
def add_limit(max_items: int) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L220)

Adds a limit `max_items` to the resource pipe

This mutates the encapsulated generator to stop after `max_items` items are yielded. This is useful for testing and debugging. It is
a no-op for transformers. Those should be limited by their input data.

**Arguments**:

- `max_items` _int_ - The maximum number of items to yield

**Returns**:

- `"DltResource"` - returns self

#### bind

```python
def bind(*args: Any, **kwargs: Any) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L276)

Binds the parametrized resource to passed arguments. Modifies resource pipe in place. Does not evaluate generators or iterators.

#### state

```python
@property
def state() -> StrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L301)

Gets resource-scoped state from the active pipeline. PipelineStateNotAvailable is raised if pipeline context is not available

#### clone

```python
def clone(clone_pipe: bool = True, keep_pipe_id: bool = True) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L306)

Creates a deep copy of a current resource, optionally cloning also pipe. Note that name of a containing source will not be cloned.

#### \_\_call\_\_

```python
def __call__(*args: Any, **kwargs: Any) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L314)

Binds the parametrized resources to passed arguments. Creates and returns a bound resource. Generators and iterators are not evaluated.

#### \_\_or\_\_

```python
def __or__(transform: Union["DltResource", AnyFun]) -> "DltResource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L321)

Allows to pipe data from across resources and transform functions with | operator

#### \_\_iter\_\_

```python
def __iter__() -> Iterator[TDataItem]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L335)

Opens iterator that yields the data items from the resources in the same order as in Pipeline class.

A read-only state is provided, initialized from active pipeline state. The state is discarded after the iterator is closed.

## DltResourceDict Objects

```python
class DltResourceDict(Dict[str, DltResource])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L445)

#### selected

```python
@property
def selected() -> Dict[str, DltResource]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L454)

Returns a subset of all resources that will be extracted and loaded to the destination.

#### extracted

```python
@property
def extracted() -> Dict[str, DltResource]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L459)

Returns a dictionary of all resources that will be extracted. That includes selected resources and all their parents.
For parents that are not added explicitly to the source, a mock resource object is created that holds the parent pipe and derives the table
schema from the child resource

#### selected\_dag

```python
@property
def selected_dag() -> List[Tuple[str, str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L484)

Returns a list of edges of directed acyclic graph of pipes and their parents in selected resources

## DltSource Objects

```python
class DltSource(Iterable[TDataItem])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L553)

Groups several `dlt resources` under a single schema and allows to perform operations on them.

### Summary
The instance of this class is created whenever you call the `dlt.source` decorated function. It automates several functions for you:
* You can pass this instance to `dlt` `run` method in order to load all data present in the `dlt resources`.
* You can select and deselect resources that you want to load via `with_resources` method
* You can access the resources (which are `DltResource` instances) as source attributes
* It implements `Iterable` interface so you can get all the data from the resources yourself and without dlt pipeline present.
* You can get the `schema` for the source and all the resources within it.
* You can use a `run` method to load the data with a default instance of dlt pipeline.
* You can get source read only state for the currently active Pipeline instance

#### from\_data

```python
@classmethod
def from_data(cls, name: str, section: str, schema: Schema,
              data: Any) -> "DltSource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L583)

Converts any `data` supported by `dlt` `run` method into `dlt source` with a name `section`.`name` and `schema` schema.

#### max\_table\_nesting

```python
@property
def max_table_nesting() -> int
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L600)

A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.

#### exhausted

```python
@property
def exhausted() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L609)

check all selected pipes wether one of them has started. if so, the source is exhausted.

#### root\_key

```python
@property
def root_key() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L619)

Enables merging on all resources by propagating root foreign key to child tables. This option is most useful if you plan to change write disposition of a resource to disable/enable merge

#### resources

```python
@property
def resources() -> DltResourceDict
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L640)

A dictionary of all resources present in the source, where the key is a resource name.

#### selected\_resources

```python
@property
def selected_resources() -> Dict[str, DltResource]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L645)

A dictionary of all the resources that are selected to be loaded.

#### discover\_schema

```python
def discover_schema(item: TDataItem = None) -> Schema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L657)

Computes table schemas for all selected resources in the source and merges them with a copy of current source schema. If `item` is provided,
dynamic tables will be evaluated, otherwise those tables will be ignored.

#### with\_resources

```python
def with_resources(*resource_names: str) -> "DltSource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L670)

A convenience method to select one of more resources to be loaded. Returns a clone of the original source with the specified resources selected.

#### decompose

```python
def decompose(strategy: TDecompositionStrategy) -> List["DltSource"]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L676)

Decomposes source into a list of sources with a given strategy.

"none" will return source as is
"scc" will decompose the dag of selected pipes and their parent into strongly connected components

#### add\_limit

```python
def add_limit(max_items: int) -> "DltSource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L693)

Adds a limit `max_items` yielded from all selected resources in the source that are not transformers.

This is useful for testing, debugging and generating sample datasets for experimentation. You can easily get your test dataset in a few minutes, when otherwise
you'd need to wait hours for the full loading to complete.

**Arguments**:

- `max_items` _int_ - The maximum number of items to yield

**Returns**:

- `"DltSource"` - returns self

#### run

```python
@property
def run() -> SupportsPipelineRun
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L709)

A convenience method that will call `run` run on the currently active `dlt` pipeline. If pipeline instance is not found, one with default settings will be created.

#### state

```python
@property
def state() -> StrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L715)

Gets source-scoped state from the active pipeline. PipelineStateNotAvailable is raised if no pipeline is active

#### clone

```python
def clone() -> "DltSource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L720)

Creates a deep copy of the source where copies of schema, resources and pipes are created

#### \_\_iter\_\_

```python
def __iter__() -> Iterator[TDataItem]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/extract/source.py#L725)

Opens iterator that yields the data items from all the resources within the source in the same order as in Pipeline class.

A read-only state is provided, initialized from active pipeline state. The state is discarded after the iterator is closed.

A source config section is injected to allow secrets/config injection as during regular extraction.

