---
sidebar_label: toml
title: common.configuration.providers.toml
---

## BaseDocProvider Objects

```python
class BaseDocProvider(ConfigProvider)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/providers/toml.py#L19)

### set\_value

```python
def set_value(key: str, value: Any, pipeline_name: Optional[str],
              *sections: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/providers/toml.py#L44)

Sets `value` under `key` in `sections` and optionally for `pipeline_name`

If key already has value of type dict and value to set is also of type dict, the new value
is merged with old value.

### set\_fragment

```python
def set_fragment(key: Optional[str], value_or_fragment: str,
                 pipeline_name: str, *sections: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/providers/toml.py#L73)

Tries to interpret `value_or_fragment` as a fragment of toml, yaml or json string and replace/merge into config doc.

If `key` is not provided, fragment is considered a full document and will replace internal config doc. Otherwise
fragment is merged with config doc from the root element and not from the element under `key`!

For simple values it falls back to `set_value` method.

## CustomLoaderDocProvider Objects

```python
class CustomLoaderDocProvider(BaseDocProvider)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/providers/toml.py#L135)

### \_\_init\_\_

```python
def __init__(name: str,
             loader: Callable[[], Dict[str, Any]],
             supports_secrets: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/providers/toml.py#L136)

Provider that calls `loader` function to get a Python dict with config/secret values to be queried.
The `loader` function typically loads a string (ie. from file), parses it (ie. as toml or yaml), does additional
processing and returns a Python dict to be queried.

Instance of CustomLoaderDocProvider must be registered for the returned dict to be used to resolve config values.

```py
import dlt
dlt.config.register_provider(provider)
```

**Arguments**:

- `name(str)` - name of the provider that will be visible ie. in exceptions
  loader(Callable[[], Dict[str, Any]]): user-supplied function that will load the document with config/secret values
- `supports_secrets(bool)` - allows to store secret values in this provider

## ProjectDocProvider Objects

```python
class ProjectDocProvider(CustomLoaderDocProvider)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/providers/toml.py#L170)

### \_\_init\_\_

```python
def __init__(name: str,
             supports_secrets: bool,
             file_name: str,
             project_dir: str = None,
             add_global_config: bool = False) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/providers/toml.py#L171)

Creates config provider from a `toml` file

The provider loads the `toml` file with specified name and from specified folder. If `add_global_config` flags is specified,
it will look for `file_name` in `dlt` home dir. The "project" (`project_dir`) values overwrite the "global" values.

If none of the files exist, an empty provider is created.

**Arguments**:

- `name(str)` - name of the provider when registering in context
- `supports_secrets(bool)` - allows to store secret values in this provider
- `file_name` _str_ - The name of `toml` file to load
- `project_dir` _str, optional_ - The location of `file_name`. If not specified, defaults to $cwd/.dlt
- `add_global_config` _bool, optional_ - Looks for `file_name` in `dlt` home directory which in most cases is $HOME/.dlt
  

**Raises**:

- `TomlProviderReadException` - File could not be read, most probably `toml` parsing error

