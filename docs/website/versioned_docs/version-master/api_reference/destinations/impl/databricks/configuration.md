---
sidebar_label: configuration
title: destinations.impl.databricks.configuration
---

## DatabricksCredentials Objects

```python
@configspec
class DatabricksCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/databricks/configuration.py#L13)

### session\_configuration

Dict of session parameters that will be passed to `databricks.sql.connect`

### connection\_parameters

Additional keyword arguments that are passed to `databricks.sql.connect`

## DatabricksClientConfiguration Objects

```python
@configspec
class DatabricksClientConfiguration(
        DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/databricks/configuration.py#L53)

### destination\_type

type: ignore[misc]

### staging\_credentials\_name

If set, credentials with given name will be used in copy command

### is\_staging\_external\_location

If true, the temporary credentials are not propagated to the COPY command

### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/databricks/configuration.py#L61)

Return displayable destination location

