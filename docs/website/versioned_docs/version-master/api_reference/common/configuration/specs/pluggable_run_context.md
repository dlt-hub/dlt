---
sidebar_label: pluggable_run_context
title: common.configuration.specs.pluggable_run_context
---

## SupportsRunContext Objects

```python
class SupportsRunContext(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L9)

Describes where `dlt` looks for settings, pipeline working folder. Implementations must be picklable.

### \_\_init\_\_

```python
def __init__(run_dir: Optional[str], *args: Any, **kwargs: Any)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L12)

An explicit run_dir, if None, run_dir should be auto-detected by particular implementation

### name

```python
@property
def name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L16)

Name of the run context. Entities like sources and destinations added to registries when this context
is active, will be scoped to it. Typically corresponds to Python package name ie. `dlt`.

### global\_dir

```python
@property
def global_dir() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L22)

Directory in which global settings are stored ie ~/.dlt/

### run\_dir

```python
@property
def run_dir() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L26)

Defines the current working directory

### settings\_dir

```python
@property
def settings_dir() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L30)

Defines where the current settings (secrets and configs) are located

### data\_dir

```python
@property
def data_dir() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L34)

Defines where the pipelines working folders are stored.

### runtime\_kwargs

```python
@property
def runtime_kwargs() -> Dict[str, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L38)

Additional kwargs used to initialize this instance of run context, used for reloading

### initial\_providers

```python
def initial_providers() -> List[ConfigProvider]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L41)

Returns initial providers for this context

### get\_data\_entity

```python
def get_data_entity(entity: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L44)

Gets path in data_dir where `entity` (ie. `pipelines`, `repos`) are stored

### get\_run\_entity

```python
def get_run_entity(entity: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L47)

Gets path in run_dir where `entity` (ie. `sources`, `destinations` etc.) are stored

### get\_setting

```python
def get_setting(setting_path: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L50)

Gets path in settings_dir where setting (ie. `secrets.toml`) are stored

## PluggableRunContext Objects

```python
class PluggableRunContext(ContainerInjectableContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L54)

Injectable run context taken via plugin

### reload

```python
def reload(run_dir: Optional[str] = None,
           runtime_kwargs: Dict[str, Any] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/pluggable_run_context.py#L76)

Reloads the context, using existing settings if not overwritten with method args

