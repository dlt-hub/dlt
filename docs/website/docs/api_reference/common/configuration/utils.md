---
sidebar_label: utils
title: common.configuration.utils
---

#### add\_config\_to\_env

```python
def add_config_to_env(
    config: BaseConfiguration, sections: Tuple[str, ...] = ()) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/utils.py#L135)

Writes values in configuration back into environment using the naming convention of EnvironProvider. Will descend recursively if embedded BaseConfiguration instances are found

#### add\_config\_dict\_to\_env

```python
def add_config_dict_to_env(dict_: Mapping[str, Any],
                           sections: Tuple[str, ...] = (),
                           overwrite_keys: bool = False) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/utils.py#L142)

Writes values in dict_ back into environment using the naming convention of EnvironProvider. Applies `sections` if specified. Does not overwrite existing keys by default

