---
sidebar_label: source
title: extract.source
---

## DltResourceDict Objects

```python
class DltResourceDict(Dict[str, DltResource])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L50)

### selected

```python
@property
def selected() -> Dict[str, DltResource]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L62)

Returns a subset of all resources that will be extracted and loaded to the destination.

### extracted

```python
@property
def extracted() -> Dict[str, DltResource]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L67)

Returns a dictionary of all resources that will be extracted. That includes selected resources and all their parents.
For parents that are not added explicitly to the source, a mock resource object is created that holds the parent pipe and derives the table
schema from the child resource

### selected\_dag

```python
@property
def selected_dag() -> List[Tuple[str, str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L91)

Returns a list of edges of directed acyclic graph of pipes and their parents in selected resources

### select

```python
def select(*resource_names: str) -> Dict[str, DltResource]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L117)

Selects `resource_name` to be extracted, and unselects remaining resources.

### detach

```python
def detach(resource_name: str = None) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L144)

Clones `resource_name` (including parent resource pipes) and removes source contexts.
Defaults to the first resource in the source if `resource_name` is None.

## DltSource Objects

```python
class DltSource(Iterable[TDataItem])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L193)

Groups several `dlt resources` under a single schema and allows to perform operations on them.

The instance of this class is created whenever you call the `dlt.source` decorated function. It automates several functions for you:
* You can pass this instance to `dlt` `run` method in order to load all data present in the `dlt resources`.
* You can select and deselect resources that you want to load via `with_resources` method
* You can access the resources (which are `DltResource` instances) as source attributes
* It implements `Iterable` interface so you can get all the data from the resources yourself and without dlt pipeline present.
* It will create a DAG from resources and transformers and optimize the extraction so parent resources are extracted only once
* You can get the `schema` for the source and all the resources within it.
* You can use a `run` method to load the data with a default instance of dlt pipeline.
* You can get source read only state for the currently active Pipeline instance

### from\_data

```python
@classmethod
def from_data(cls, schema: Schema, section: str, data: Any) -> Self
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L219)

Converts any `data` supported by `dlt` `run` method into `dlt source` with a name `section`.`name` and `schema` schema.

### max\_table\_nesting

```python
@property
def max_table_nesting() -> int
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L239)

A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.

### root\_key

```python
@property
def root_key() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L253)

Enables merging on all resources by propagating root foreign key to nested tables. This option is most useful if you plan to change write disposition of a resource to disable/enable merge

### exhausted

```python
@property
def exhausted() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L298)

Check all selected pipes whether one of them has started. if so, the source is exhausted.

### resources

```python
@property
def resources() -> DltResourceDict
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L308)

A dictionary of all resources present in the source, where the key is a resource name.

### selected\_resources

```python
@property
def selected_resources() -> Dict[str, DltResource]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L313)

A dictionary of all the resources that are selected to be loaded.

### discover\_schema

```python
def discover_schema(item: TDataItem = None) -> Schema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L325)

Computes table schemas for all selected resources in the source and merges them with a copy of current source schema. If `item` is provided,
dynamic tables will be evaluated, otherwise those tables will be ignored.

### with\_resources

```python
def with_resources(*resource_names: str) -> "DltSource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L338)

A convenience method to select one of more resources to be loaded. Returns a clone of the original source with the specified resources selected.

### decompose

```python
def decompose(strategy: TDecompositionStrategy) -> List["DltSource"]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L344)

Decomposes source into a list of sources with a given strategy.

"none" will return source as is
"scc" will decompose the dag of selected pipes and their parent into strongly connected components

### add\_limit

```python
def add_limit(max_items: int) -> "DltSource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L361)

Adds a limit `max_items` yielded from all selected resources in the source that are not transformers.

This is useful for testing, debugging and generating sample datasets for experimentation. You can easily get your test dataset in a few minutes, when otherwise
you'd need to wait hours for the full loading to complete.

**Notes**:

  1. Transformers resources won't be limited. They should process all the data they receive fully to avoid inconsistencies in generated datasets.
  2. Each yielded item may contain several records. `add_limit` only limits the "number of yields", not the total number of records.
  

**Arguments**:

- `max_items` _int_ - The maximum number of items to yield

**Returns**:

- `"DltSource"` - returns self

### parallelize

```python
def parallelize() -> "DltSource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L380)

Mark all resources in the source to run in parallel.

Only transformers and resources based on generators and generator functions are supported, unsupported resources will be skipped.

### run

```python
@property
def run() -> SupportsPipelineRun
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L393)

A convenience method that will call `run` run on the currently active `dlt` pipeline. If pipeline instance is not found, one with default settings will be created.

### state

```python
@property
def state() -> StrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L401)

Gets source-scoped state from the active pipeline. PipelineStateNotAvailable is raised if no pipeline is active

### clone

```python
def clone(with_name: str = None) -> "DltSource"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L406)

Creates a deep copy of the source where copies of schema, resources and pipes are created.

If `with_name` is provided, a schema is cloned with a changed name

### \_\_iter\_\_

```python
def __iter__() -> Iterator[TDataItem]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L416)

Opens iterator that yields the data items from all the resources within the source in the same order as in Pipeline class.

A read-only state is provided, initialized from active pipeline state. The state is discarded after the iterator is closed.

A source config section is injected to allow secrets/config injection as during regular extraction.

## SourceFactory Objects

```python
class SourceFactory(Protocol, Generic[TSourceFunParams, TDltSourceImpl])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L491)

### \_\_call\_\_

```python
def __call__(*args: TSourceFunParams.args,
             **kwargs: TSourceFunParams.kwargs) -> TDltSourceImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L492)

Makes dlt source

### with\_args

```python
def with_args(*,
              name: str = None,
              section: str = None,
              max_table_nesting: int = None,
              root_key: bool = False,
              schema: Schema = None,
              schema_contract: TSchemaContract = None,
              spec: Type[BaseConfiguration] = None,
              parallelized: bool = None,
              _impl_cls: Type[TDltSourceImpl] = None) -> Self
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L501)

Overrides default decorator arguments that will be used to when DltSource instance and returns modified clone.

## SourceReference Objects

```python
class SourceReference()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L520)

Runtime information on the source/resource

### SOURCES

A registry of all the decorated sources and resources discovered when importing modules

### to\_fully\_qualified\_ref

```python
@staticmethod
def to_fully_qualified_ref(ref: str) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L549)

Converts ref into fully qualified form, return one or more alternatives for shorthand notations.
Run context is injected in needed.

### from\_reference

```python
@classmethod
def from_reference(cls, ref: str) -> AnySourceFactory
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/source.py#L591)

Returns registered source factory or imports source module and returns a function.
Expands shorthand notation into section.name eg. "sql_database" is expanded into "sql_database.sql_database"

