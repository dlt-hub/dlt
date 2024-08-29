---
sidebar_label: pipeline
title: common.pipeline
---

## \_StepInfo Objects

```python
class _StepInfo(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L68)

### loads\_ids

ids of the loaded packages

### load\_packages

Information on loaded packages

## StepInfo Objects

```python
class StepInfo(SupportsHumanize, Generic[TStepMetricsCo])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L82)

### metrics

Metrics per load id. If many sources with the same name were extracted, there will be more than 1 element in the list

### loads\_ids

ids of the loaded packages

### load\_packages

Information on loaded packages

### started\_at

```python
@property
def started_at() -> datetime.datetime
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L93)

Returns the earliest start date of all collected metrics

### finished\_at

```python
@property
def finished_at() -> datetime.datetime
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L103)

Returns the latest end date of all collected metrics

## \_ExtractInfo Objects

```python
class _ExtractInfo(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L182)

NamedTuple cannot be part of the derivation chain so we must re-declare all fields to use it as mixin later

### loads\_ids

ids of the loaded packages

### load\_packages

Information on loaded packages

## ExtractInfo Objects

```python
class ExtractInfo(StepInfo[ExtractMetrics], _ExtractInfo)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L195)

A tuple holding information on extracted data items. Returned by pipeline `extract` method.

### asdict

```python
def asdict() -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L198)

A dictionary representation of ExtractInfo that can be loaded with `dlt`

## \_NormalizeInfo Objects

```python
class _NormalizeInfo(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L247)

### loads\_ids

ids of the loaded packages

### load\_packages

Information on loaded packages

## NormalizeInfo Objects

```python
class NormalizeInfo(StepInfo[NormalizeMetrics], _NormalizeInfo)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L257)

A tuple holding information on normalized data items. Returned by pipeline `normalize` method.

### asdict

```python
def asdict() -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L272)

A dictionary representation of NormalizeInfo that can be loaded with `dlt`

## \_LoadInfo Objects

```python
class _LoadInfo(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L306)

### loads\_ids

ids of the loaded packages

### load\_packages

Information on loaded packages

## LoadInfo Objects

```python
class LoadInfo(StepInfo[LoadMetrics], _LoadInfo)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L325)

A tuple holding the information on recently loaded packages. Returned by pipeline `run` and `load` methods

### asdict

```python
def asdict() -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L328)

A dictionary representation of LoadInfo that can be loaded with `dlt`

### has\_failed\_jobs

```python
@property
def has_failed_jobs() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L370)

Returns True if any of the load packages has a failed job.

### raise\_on\_failed\_jobs

```python
def raise_on_failed_jobs() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L377)

Raises `DestinationHasFailedJobs` exception if any of the load packages has a failed job.

## WithStepInfo Objects

```python
class WithStepInfo(ABC, Generic[TStepMetrics, TStepInfo])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L394)

Implemented by classes that generate StepInfo with metrics and package infos

### current\_load\_id

```python
@property
def current_load_id() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L427)

Returns currently processing load id

### get\_step\_info

```python
@abstractmethod
def get_step_info(pipeline: "SupportsPipeline") -> TStepInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L432)

Returns and instance of StepInfo with metrics and package infos

## TPipelineLocalState Objects

```python
class TPipelineLocalState(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L440)

### first\_run

Indicates a first run of the pipeline, where run ends with successful loading of data

## TPipelineState Objects

```python
class TPipelineState(TVersionedState)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L449)

Schema for a pipeline state that is stored within the pipeline working directory

### default\_schema\_name

Name of the first schema added to the pipeline to which all the resources without schemas will be added

### schema\_names

All the schemas present within the pipeline working directory

## TSourceState Objects

```python
class TSourceState(TPipelineState)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L470)

### sources

type: ignore[misc]

## SupportsPipeline Objects

```python
class SupportsPipeline(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L474)

A protocol with core pipeline operations that lets high level abstractions ie. sources to access pipeline methods and properties

### pipeline\_name

Name of the pipeline

### default\_schema\_name

Name of the default schema

### destination

The destination reference which is ModuleType. `destination.__name__` returns the name string

### dataset\_name

Name of the dataset to which pipeline will be loaded to

### runtime\_config

A configuration of runtime options like logging level and format and various tracing options

### working\_dir

A working directory of the pipeline

### pipeline\_salt

A configurable pipeline secret to be used as a salt or a seed for encryption key

### first\_run

Indicates a first run of the pipeline, where run ends with successful loading of the data

### state

```python
@property
def state() -> TPipelineState
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L495)

Returns dictionary with pipeline state

### schemas

```python
@property
def schemas() -> Mapping[str, Schema]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L499)

Mapping of all pipeline schemas

### set\_local\_state\_val

```python
def set_local_state_val(key: str, value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L502)

Sets value in local state. Local state is not synchronized with destination.

### get\_local\_state\_val

```python
def get_local_state_val(key: str) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L505)

Gets value from local state. Local state is not synchronized with destination.

## PipelineContext Objects

```python
@configspec
class PipelineContext(ContainerInjectableContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L548)

### pipeline

```python
def pipeline() -> SupportsPipeline
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L558)

Creates or returns exiting pipeline

### \_\_init\_\_

```python
def __init__(
        deferred_pipeline: Callable[..., SupportsPipeline] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L585)

Initialize the context with a function returning the Pipeline object to allow creation on first use

## current\_pipeline

```python
def current_pipeline() -> SupportsPipeline
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L590)

Gets active pipeline context or None if not found

## pipeline\_state

```python
def pipeline_state(
        container: Container,
        initial_default: TPipelineState = None) -> Tuple[TPipelineState, bool]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L605)

Gets value of the state from context or active pipeline, if none found returns `initial_default`

Injected state is called "writable": it is injected by the `Pipeline` class and all the changes will be persisted.
The state coming from pipeline context or `initial_default` is called "read only" and all the changes to it will be discarded

Returns tuple (state, writable)

## source\_state

```python
def source_state() -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L646)

Returns a dictionary with the source-scoped state. Source-scoped state may be shared across the resources of a particular source. Please avoid using source scoped state. Check
the `resource_state` function for resource-scoped state that is visible within particular resource. Dlt state is preserved across pipeline runs and may be used to implement incremental loads.

**Notes**:

  The source state is a python dictionary-like object that is available within the `@dlt.source` and `@dlt.resource` decorated functions and may be read and written to.
  The data within the state is loaded into destination together with any other extracted data and made automatically available to the source/resource extractor functions when they are run next time.
  When using the state:
  * The source state is scoped to a particular source and will be stored under the source name in the pipeline state
  * It is possible to share state across many sources if they share a schema with the same name
  * Any JSON-serializable values can be written and the read from the state. `dlt` dumps and restores instances of Python bytes, DateTime, Date and Decimal types.
  * The state available in the source decorated function is read only and any changes will be discarded.
  * The state available in the resource decorated function is writable and written values will be available on the next pipeline run

## resource\_state

```python
def resource_state(resource_name: str = None,
                   source_state_: Optional[DictStrAny] = None) -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L695)

Returns a dictionary with the resource-scoped state. Resource-scoped state is visible only to resource requesting the access. Dlt state is preserved across pipeline runs and may be used to implement incremental loads.

Note that this function accepts the resource name as optional argument. There are rare cases when `dlt` is not able to resolve resource name due to requesting function
working in different thread than the main. You'll need to pass the name explicitly when you request resource_state from async functions or functions decorated with @defer.

Summary:
The resource state is a python dictionary-like object that is available within the `@dlt.resource` decorated functions and may be read and written to.
The data within the state is loaded into destination together with any other extracted data and made automatically available to the source/resource extractor functions when they are run next time.
When using the state:
* The resource state is scoped to a particular resource requesting it.
* Any JSON-serializable values can be written and the read from the state. `dlt` dumps and restores instances of Python bytes, DateTime, Date and Decimal types.
* The state available in the resource decorated function is writable and written values will be available on the next pipeline run

**Example**:

  The most typical use case for the state is to implement incremental load.
```py
    @dlt.resource(write_disposition="append")
    def players_games(chess_url, players, start_month=None, end_month=None):
        checked_archives = dlt.current.resource_state().setdefault("archives", [])
        archives = players_archives(chess_url, players)
        for url in archives:
            if url in checked_archives:
                print(f"skipping archive {url}")
                continue
            else:
                print(f"getting archive {url}")
                checked_archives.append(url)
            # get the filtered archive
            r = requests.get(url)
            r.raise_for_status()
            yield r.json().get("games", [])
```
  
  Here we store all the urls with game archives in the state and we skip loading them on next run. The archives are immutable. The state will grow with the coming months (and more players).
  Up to few thousand archives we should be good though.

**Arguments**:

- `resource_name` _str, optional_ - forces to use state for a resource with this name. Defaults to None.
- `source_state_` _Optional[DictStrAny], optional_ - Alternative source state. Defaults to None.
  

**Raises**:

- `ResourceNameNotAvailable` - Raise if used outside of resource context or from a different thread than main
  

**Returns**:

- `DictStrAny` - State dictionary

## reset\_resource\_state

```python
def reset_resource_state(resource_name: str,
                         source_state_: Optional[DictStrAny] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L748)

Resets the resource state with name `resource_name` by removing it from `source_state`

**Arguments**:

- `resource_name` - The resource key to reset
- `state` - Optional source state dictionary to operate on. Use when working outside source context.

## get\_dlt\_pipelines\_dir

```python
def get_dlt_pipelines_dir() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L778)

Gets default directory where pipelines' data will be stored
1. in user home directory ~/.dlt/pipelines/
2. if current user is root in /var/dlt/pipelines
3. if current user does not have a home directory in /tmp/dlt/pipelines

## get\_dlt\_repos\_dir

```python
def get_dlt_repos_dir() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/pipeline.py#L787)

Gets default directory where command repositories will be stored

