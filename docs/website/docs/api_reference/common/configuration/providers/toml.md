---
sidebar_label: toml
title: common.configuration.providers.toml
---

## VaultTomlProvider Objects

```python
class VaultTomlProvider(BaseTomlProvider)
```

A toml-backed Vault abstract config provider.

This provider allows implementation of providers that store secrets in external vaults: like Hashicorp, Google Secrets or Airflow Metadata.
The basic working principle is obtain config and secrets values from Vault keys and reconstitute a `secrets.toml` like document that is then used
as a cache.

The implemented must provide `_look_vault` method that returns a value from external vault from external key.

To reduce number of calls to external vaults the provider is searching for a known configuration fragments which should be toml documents and merging
them with the
- only keys with secret type hint (CredentialsConfiguration, TSecretValue) will be looked up by default.
- provider gathers `toml` document fragments that contain source and destination credentials in path specified below
- single values will not be retrieved, only toml fragments by default

#### \_\_init\_\_

```python
def __init__(only_secrets: bool, only_toml_fragments: bool) -> None
```

Initializes the toml backed Vault provider by loading a toml fragment from `dlt_secrets_toml` key and using it as initial configuration.

_extended_summary_

**Arguments**:

- `only_secrets` _bool_ - Only looks for secret values (CredentialsConfiguration, TSecretValue) by returning None (not found)
- `only_toml_fragments` _bool_ - Only load the known toml fragments and ignore any other lookups by returning None (not found)

## TomlFileProvider Objects

```python
class TomlFileProvider(BaseTomlProvider)
```

#### \_\_init\_\_

```python
def __init__(file_name: str,
             project_dir: str = None,
             add_global_config: bool = False) -> None
```

Creates config provider from a `toml` file

The provider loads the `toml` file with specified name and from specified folder. If `add_global_config` flags is specified,
it will look for `file_name` in `dlt` home dir. The "project" (`project_dir`) values overwrite the "global" values.

If none of the files exist, an empty provider is created.

**Arguments**:

- `file_name` _str_ - The name of `toml` file to load
- `project_dir` _str, optional_ - The location of `file_name`. If not specified, defaults to $cwd/.dlt
- `add_global_config` _bool, optional_ - Looks for `file_name` in `dlt` home directory which in most cases is $HOME/.dlt
  

**Raises**:

- `TomlProviderReadException` - File could not be read, most probably `toml` parsing error

