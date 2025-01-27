---
sidebar_label: utils
title: common.configuration.utils
---

## auto\_cast

```python
def auto_cast(value: str) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/utils.py#L121)

Parse and cast str `value` to bool, int, float and complex (via JSON)

F[f]alse and T[t]rue strings are cast to bool values

## auto\_config\_fragment

```python
def auto_config_fragment(value: str) -> Optional[DictStrAny]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/utils.py#L142)

Tries to parse config fragment assuming toml, yaml and json formats

Only dicts are considered valid fragments.
None is returned when not a fragment

## add\_config\_to\_env

```python
def add_config_to_env(
    config: BaseConfiguration, sections: Tuple[str, ...] = ()) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/utils.py#L197)

Writes values in configuration back into environment using the naming convention of EnvironProvider. Will descend recursively if embedded BaseConfiguration instances are found

## add\_config\_dict\_to\_env

```python
def add_config_dict_to_env(dict_: Mapping[str, Any],
                           sections: Tuple[str, ...] = (),
                           overwrite_keys: bool = False,
                           destructure_dicts: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/utils.py#L204)

Writes values in dict_ back into environment using the naming convention of EnvironProvider. Applies `sections` if specified. Does not overwrite existing keys by default

