---
sidebar_label: vault
title: common.configuration.providers.vault
---

## VaultDocProvider Objects

```python
class VaultDocProvider(BaseDocProvider)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/providers/vault.py#L15)

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

### \_\_init\_\_

```python
def __init__(only_secrets: bool, only_toml_fragments: bool) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/providers/vault.py#L32)

Initializes the toml backed Vault provider by loading a toml fragment from `dlt_secrets_toml` key and using it as initial configuration.

_extended_summary_

**Arguments**:

- `only_secrets` _bool_ - Only looks for secret values (CredentialsConfiguration, TSecretValue) by returning None (not found)
- `only_toml_fragments` _bool_ - Only load the known toml fragments and ignore any other lookups by returning None (not found)

