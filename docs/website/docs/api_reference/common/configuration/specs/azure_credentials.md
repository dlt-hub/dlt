---
sidebar_label: azure_credentials
title: common.configuration.specs.azure_credentials
---

## AzureCredentialsWithoutDefaults Objects

```python
@configspec
class AzureCredentialsWithoutDefaults(CredentialsConfiguration)
```

Credentials for azure blob storage, compatible with adlfs

#### azure\_sas\_token\_permissions

Permissions to use when generating a SAS token. Ignored when sas token is provided directly

#### to\_adlfs\_credentials

```python
def to_adlfs_credentials() -> Dict[str, Any]
```

Return a dict that can be passed as kwargs to adlfs

