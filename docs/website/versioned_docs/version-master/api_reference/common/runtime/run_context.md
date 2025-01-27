---
sidebar_label: run_context
title: common.runtime.run_context
---

## RunContext Objects

```python
class RunContext(SupportsRunContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/run_context.py#L23)

A default run context used by dlt

### run\_dir

```python
@property
def run_dir() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/run_context.py#L36)

The default run dir is the current working directory but may be overridden by DLT_PROJECT_DIR env variable.

### settings\_dir

```python
@property
def settings_dir() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/run_context.py#L41)

Returns a path to dlt settings directory. If not overridden it resides in current working directory

The name of the setting folder is '.dlt'. The path is current working directory '.' but may be overridden by DLT_PROJECT_DIR env variable.

### data\_dir

```python
@property
def data_dir() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/run_context.py#L49)

Gets default directory where pipelines' data (working directories) will be stored
1. if DLT_DATA_DIR is set in env then it is used
2. in user home directory: ~/.dlt/
3. if current user is root: in /var/dlt/
4. if current user does not have a home directory: in /tmp/dlt/

### get\_run\_entity

```python
def get_run_entity(entity: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/run_context.py#L87)

Default run context assumes that entities are defined in root dir

## plug\_run\_context

```python
@plugins.hookspec(firstresult=True)
def plug_run_context(
        run_dir: Optional[str],
        runtime_kwargs: Optional[Dict[str, Any]]) -> SupportsRunContext
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/run_context.py#L100)

Spec for plugin hook that returns current run context.

**Arguments**:

- `run_dir` _str_ - An initial run directory of the context
- `runtime_kwargs` - Any additional arguments passed to the context via PluggableRunContext.reload
  

**Returns**:

- `SupportsRunContext` - A run context implementing SupportsRunContext protocol

## current

```python
def current() -> SupportsRunContext
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/run_context.py#L136)

Returns currently active run context

