---
sidebar_label: athena
title: destinations.impl.athena.athena
---

## AthenaClient Objects

```python
class AthenaClient(SqlJobClientWithStaging, SupportsStagingDestination)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/athena/athena.py#L369)

### create\_load\_job

```python
def create_load_job(table: TTableSchema,
                    file_path: str,
                    load_id: str,
                    restore: bool = False) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/athena/athena.py#L471)

Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs

### should\_load\_data\_to\_staging\_dataset\_on\_staging\_destination

```python
def should_load_data_to_staging_dataset_on_staging_destination(
        table: TTableSchema) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/athena/athena.py#L536)

iceberg table data goes into staging on staging destination

