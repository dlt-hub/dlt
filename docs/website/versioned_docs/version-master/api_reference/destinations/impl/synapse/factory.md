---
sidebar_label: factory
title: destinations.impl.synapse.factory
---

## synapse Objects

```python
class synapse(Destination[SynapseClientConfiguration, "SynapseClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/synapse/factory.py#L39)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[SynapseCredentials, t.Dict[str, t.Any],
                                  str] = None,
             default_table_index_type: t.Optional[TTableIndexType] = "heap",
             create_indexes: bool = False,
             staging_use_msi: bool = False,
             has_case_sensitive_identifiers: bool = False,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/synapse/factory.py#L113)

Configure the Synapse destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the Synapse dedicated pool. Can be an instance of `SynapseCredentials` or
  a connection string in the format `synapse://user:password@host:port/database`
- `default_table_index_type` - Maps directly to the default_table_index_type attribute of the SynapseClientConfiguration object.
- `create_indexes` - Maps directly to the create_indexes attribute of the SynapseClientConfiguration object.
- `staging_use_msi` - Maps directly to the staging_use_msi attribute of the SynapseClientConfiguration object.
- `has_case_sensitive_identifiers` - Are identifiers used by synapse database case sensitive (following the catalog collation)
- `**kwargs` - Additional arguments passed to the destination config

