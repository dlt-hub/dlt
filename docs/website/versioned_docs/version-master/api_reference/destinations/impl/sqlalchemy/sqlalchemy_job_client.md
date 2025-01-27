---
sidebar_label: sqlalchemy_job_client
title: destinations.impl.sqlalchemy.sqlalchemy_job_client
---

## SqlalchemyJobClient Objects

```python
class SqlalchemyJobClient(SqlJobClientWithStagingDataset)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/sqlalchemy/sqlalchemy_job_client.py#L37)

### sql\_client

type: ignore[assignment]

### get\_stored\_schema

```python
def get_stored_schema(schema_name: str = None) -> Optional[StorageSchemaInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/sqlalchemy/sqlalchemy_job_client.py#L272)

Get the latest stored schema

