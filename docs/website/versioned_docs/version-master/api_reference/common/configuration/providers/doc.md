---
sidebar_label: doc
title: common.configuration.providers.doc
---

## BaseDocProvider Objects

```python
class BaseDocProvider(ConfigProvider)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/providers/doc.py#L11)

### set\_value

```python
def set_value(key: str, value: Any, pipeline_name: Optional[str],
              *sections: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/providers/doc.py#L39)

Sets `value` under `key` in `sections` and optionally for `pipeline_name`

If key already has value of type dict and value to set is also of type dict, the new value
is merged with old value.

### set\_fragment

```python
def set_fragment(key: Optional[str], value_or_fragment: str,
                 pipeline_name: str, *sections: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/providers/doc.py#L47)

Tries to interpret `value_or_fragment` as a fragment of toml, yaml or json string and replace/merge into config doc.

If `key` is not provided, fragment is considered a full document and will replace internal config doc. Otherwise
fragment is merged with config doc from the root element and not from the element under `key`!

For simple values it falls back to `set_value` method.

## CustomLoaderDocProvider Objects

```python
class CustomLoaderDocProvider(BaseDocProvider)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/providers/doc.py#L137)

### \_\_init\_\_

```python
def __init__(name: str,
             loader: Callable[[], Dict[str, Any]],
             supports_secrets: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/providers/doc.py#L138)

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

