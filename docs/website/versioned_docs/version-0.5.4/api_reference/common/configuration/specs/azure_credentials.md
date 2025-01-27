---
sidebar_label: azure_credentials
title: common.configuration.specs.azure_credentials
---

## AzureCredentialsWithoutDefaults Objects

```python
@configspec
class AzureCredentialsWithoutDefaults(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/azure_credentials.py#L13)

Credentials for azure blob storage, compatible with adlfs

### azure\_sas\_token\_permissions

Permissions to use when generating a SAS token. Ignored when sas token is provided directly

### to\_adlfs\_credentials

```python
def to_adlfs_credentials() -> Dict[str, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/azure_credentials.py#L22)

Return a dict that can be passed as kwargs to adlfs

