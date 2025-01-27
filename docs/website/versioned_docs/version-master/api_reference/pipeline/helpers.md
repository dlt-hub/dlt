---
sidebar_label: helpers
title: pipeline.helpers
---

## retry\_load

```python
def retry_load(retry_on_pipeline_steps: Sequence[TPipelineStep] = (
    "load", )) -> Callable[[BaseException], bool]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/pipeline/helpers.py#L30)

A retry strategy for Tenacity that, with default setting, will repeat `load` step for all exceptions that are not terminal

Use this condition with tenacity `retry_if_exception`. Terminal exceptions are exceptions that will not go away when operations is repeated.
Examples: missing configuration values, Authentication Errors, terminally failed jobs exceptions etc.

```py

data = source(...)
for attempt in Retrying(stop=stop_after_attempt(3), retry=retry_if_exception(retry_load(())), reraise=True):
    with attempt:
        p.run(data)
```

**Arguments**:

- `retry_on_pipeline_steps` _Tuple[TPipelineStep, ...], optional_ - which pipeline steps are allowed to be repeated. Default: "load"

## DropCommand Objects

```python
class DropCommand()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/pipeline/helpers.py#L62)

### \_\_init\_\_

```python
def __init__(pipeline: "Pipeline",
             resources: Union[Iterable[Union[str, TSimpleRegex]],
                              Union[str, TSimpleRegex]] = (),
             schema_name: Optional[str] = None,
             state_paths: TAnyJsonPath = (),
             drop_all: bool = False,
             state_only: bool = False) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/pipeline/helpers.py#L63)

**Arguments**:

- `pipeline` - Pipeline to drop tables and state from
- `resources` - List of resources to drop. If empty, no resources are dropped unless `drop_all` is True
- `schema_name` - Name of the schema to drop tables from. If not specified, the default schema is used
- `state_paths` - JSON path(s) relative to the source state to drop
- `drop_all` - Drop all resources and tables in the schema (supersedes `resources` list)
- `state_only` - Drop only state, not tables

## refresh\_source

```python
def refresh_source(pipeline: "Pipeline", source: DltSource,
                   refresh: TRefreshMode) -> TLoadPackageDropTablesState
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/pipeline/helpers.py#L161)

Run the pipeline's refresh mode on the given source, updating the provided `schema` and pipeline state.

**Returns**:

  The new load package state containing tables that need to be dropped/truncated.

