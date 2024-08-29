---
sidebar_label: athena
title: destinations.impl.athena.athena
---

## AthenaClient Objects

```python
class AthenaClient(SqlJobClientWithStaging, SupportsStagingDestination)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/athena/athena.py#L305)

### start\_file\_load

```python
def start_file_load(table: TTableSchema, file_path: str,
                    load_id: str) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/athena/athena.py#L379)

Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs

### should\_load\_data\_to\_staging\_dataset\_on\_staging\_destination

```python
def should_load_data_to_staging_dataset_on_staging_destination(
        table: TTableSchema) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/athena/athena.py#L435)

iceberg table data goes into staging on staging destination

