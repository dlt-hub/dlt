---
sidebar_label: athena
title: destinations.impl.athena.athena
---

## AthenaClient Objects

```python
class AthenaClient(SqlJobClientWithStagingDataset, SupportsStagingDestination)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/athena/athena.py#L311)

### create\_load\_job

```python
def create_load_job(table: PreparedTableSchema,
                    file_path: str,
                    load_id: str,
                    restore: bool = False) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/athena/athena.py#L414)

Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs

### should\_load\_data\_to\_staging\_dataset\_on\_staging\_destination

```python
def should_load_data_to_staging_dataset_on_staging_destination(
        table_name: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/athena/athena.py#L481)

iceberg table data goes into staging on staging destination

