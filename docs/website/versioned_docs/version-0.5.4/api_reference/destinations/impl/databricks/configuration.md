---
sidebar_label: configuration
title: destinations.impl.databricks.configuration
---

## DatabricksCredentials Objects

```python
@configspec
class DatabricksCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/databricks/configuration.py#L10)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/databricks/configuration.py#L42)

### destination\_type

type: ignore[misc]

### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/databricks/configuration.py#L46)

Return displayable destination location

