---
sidebar_label: configuration
title: destinations.impl.databricks.configuration
---

## DatabricksCredentials Objects

```python
@configspec
class DatabricksCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/databricks/configuration.py#L11)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/databricks/configuration.py#L43)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/databricks/configuration.py#L51)

Return displayable destination location

