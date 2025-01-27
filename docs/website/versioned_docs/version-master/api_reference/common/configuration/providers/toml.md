---
sidebar_label: toml
title: common.configuration.providers.toml
---

## SettingsTomlProvider Objects

```python
class SettingsTomlProvider(CustomLoaderDocProvider)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/providers/toml.py#L39)

### \_\_init\_\_

```python
def __init__(name: str,
             supports_secrets: bool,
             file_name: str,
             settings_dir: str,
             global_dir: str = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/providers/toml.py#L43)

Creates config provider from a `toml` file

The provider loads the `toml` file with specified name and from specified folder. If `global_dir` is specified,
it will additionally look for `file_name` in `dlt` global dir (home dir by default) and merge the content.
The "settings" (`settings_dir`) values overwrite the "global" values.

If toml file under `settings_dir` is not found it will look into Google Colab userdata object for a value
with name `file_name` and load toml file from it.
If that one is not found, it will try to load Streamlit `secrets.toml` file.

If none of the files exist, an empty provider is created.

**Arguments**:

- `name(str)` - name of the provider when registering in context
- `supports_secrets(bool)` - allows to store secret values in this provider
- `file_name` _str_ - The name of `toml` file to load
- `settings_dir` _str, optional_ - The location of `file_name`. If not specified, defaults to $cwd/.dlt
- `global_dir` _bool, optional_ - Looks for `file_name` in global_dir (defaults to `dlt` home directory which in most cases is $HOME/.dlt)
  

**Raises**:

- `TomlProviderReadException` - File could not be read, most probably `toml` parsing error

