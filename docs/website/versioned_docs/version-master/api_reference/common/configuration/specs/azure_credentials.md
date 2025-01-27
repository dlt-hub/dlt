---
sidebar_label: azure_credentials
title: common.configuration.specs.azure_credentials
---

## AzureCredentialsBase Objects

```python
@configspec
class AzureCredentialsBase(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/azure_credentials.py#L18)

### azure\_account\_host

Alternative host when accessing blob storage endpoint ie. my_account.dfs.core.windows.net

## AzureCredentialsWithoutDefaults Objects

```python
@configspec
class AzureCredentialsWithoutDefaults(AzureCredentialsBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/azure_credentials.py#L35)

Credentials for Azure Blob Storage, compatible with adlfs

### azure\_sas\_token\_permissions

Permissions to use when generating a SAS token. Ignored when sas token is provided directly

### to\_adlfs\_credentials

```python
def to_adlfs_credentials() -> Dict[str, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/azure_credentials.py#L43)

Return a dict that can be passed as kwargs to adlfs

