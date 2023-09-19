---
sidebar_label: incremental
title: extract.incremental
---

## Incremental Objects

```python
@configspec
class Incremental(FilterItem, BaseConfiguration, Generic[TCursorValue])
```

Adds incremental extraction for a resource by storing a cursor value in persistent state.

The cursor could for example be a timestamp for when the record was created and you can use this to load only
new records created since the last run of the pipeline.

To use this the resource function should have an argument either type annotated with `Incremental` or a default `Incremental` instance.
For example:

>>> @dlt.resource(primary_key='id')
>>> def some_data(created_at=dlt.sources.incremental('created_at', '2023-01-01T00:00:00Z'):
>>>    yield from request_data(created_after=created_at.last_value)

When the resource has a `primary_key` specified this is used to deduplicate overlapping items with the same cursor value.

Alternatively you can use this class as transform step and add it to any resource. For example:
>>> @dlt.resource
>>> def some_data():
>>>     last_value = dlt.sources.incremental.from_existing_state("some_data", "item.ts")
>>>     ...
>>>
>>> r = some_data().add_step(dlt.sources.incremental("item.ts", initial_value=now, primary_key="delta"))
>>> info = p.run(r, destination="duckdb")

**Arguments**:

- `cursor_path` - The name or a JSON path to an cursor field. Uses the same names of fields as in your JSON document, before they are normalized to store in the database.
- `initial_value` - Optional value used for `last_value` when no state is available, e.g. on the first run of the pipeline. If not provided `last_value` will be `None` on the first run.
- `last_value_func` - Callable used to determine which cursor value to save in state. It is called with a list of the stored state value and all cursor vals from currently processing items. Default is `max`
- `primary_key` - Optional primary key used to deduplicate data. If not provided, a primary key defined by the resource will be used. Pass a tuple to define a compound key. Pass empty tuple to disable unique checks
- `end_value` - Optional value used to load a limited range of records between `initial_value` and `end_value`.
  Use in conjunction with `initial_value`, e.g. load records from given month `incremental(initial_value="2022-01-01T00:00:00Z", end_value="2022-02-01T00:00:00Z")`
  Note, when this is set the incremental filtering is stateless and `initial_value` always supersedes any previous incremental value in state.
- `allow_external_schedulers` - If set to True, allows dlt to look for external schedulers from which it will take "initial_value" and "end_value" resulting in loading only
  specified range of data. Currently Airflow scheduler is detected: "data_interval_start" and "data_interval_end" are taken from the context and passed Incremental class.
  The values passed explicitly to Incremental will be ignored.
  Note that if logical "end date" is present then also "end_value" will be set which means that resource state is not used and exactly this range of date will be loaded

#### from\_existing\_state

```python
@classmethod
def from_existing_state(cls, resource_name: str,
                        cursor_path: str) -> "Incremental[TCursorValue]"
```

Create Incremental instance from existing state.

#### merge

```python
def merge(other: "Incremental[TCursorValue]") -> "Incremental[TCursorValue]"
```

Create a new incremental instance which merges the two instances.
Only properties which are not `None` from `other` override the current instance properties.

This supports use cases with partial overrides, such as:
>>> def my_resource(updated=incremental('updated', initial_value='1970-01-01'))
>>>     ...
>>>
>>> my_resource(updated=incremental(initial_value='2023-01-01', end_value='2023-02-01'))

#### get\_state

```python
def get_state() -> IncrementalColumnState
```

Returns an Incremental state for a particular cursor column

#### get\_incremental\_value\_type

```python
def get_incremental_value_type() -> Type[Any]
```

Infers the type of incremental value from a class of an instance if those preserve the Generic arguments information.

#### bind

```python
def bind(pipe: SupportsPipe) -> "Incremental[TCursorValue]"
```

Called by pipe just before evaluation

## IncrementalResourceWrapper Objects

```python
class IncrementalResourceWrapper(FilterItem)
```

#### \_\_init\_\_

```python
def __init__(
        resource_name: str,
        primary_key: Optional[TTableHintTemplate[TColumnNames]] = None
) -> None
```

Creates a wrapper over a resource function that accepts Incremental instance in its argument to perform incremental loading.

The wrapper delays instantiation of the Incremental to the moment of actual execution and is currently used by `dlt.resource` decorator.
The wrapper explicitly (via `resource_name`) parameter binds the Incremental state to a resource state.
Note that wrapper implements `FilterItem` transform interface and functions as a processing step in the before-mentioned resource pipe.

**Arguments**:

- `resource_name` _str_ - A name of resource to which the Incremental will be bound at execution
- `primary_key` _TTableHintTemplate[TColumnKey], optional_ - A primary key to be passed to Incremental Instance at execution. Defaults to None.

#### wrap

```python
def wrap(sig: inspect.Signature, func: TFun) -> TFun
```

Wrap the callable to inject an `Incremental` object configured for the resource.

#### allow\_external\_schedulers

```python
@property
def allow_external_schedulers() -> bool
```

Allows the Incremental instance to get its initial and end values from external schedulers like Airflow

