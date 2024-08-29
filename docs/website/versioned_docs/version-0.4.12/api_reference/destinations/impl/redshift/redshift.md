---
sidebar_label: redshift
title: destinations.impl.redshift.redshift
---

## RedshiftMergeJob Objects

```python
class RedshiftMergeJob(SqlMergeFollowupJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/redshift/redshift.py#L199)

### gen\_key\_table\_clauses

```python
@classmethod
def gen_key_table_clauses(cls, root_table_name: str,
                          staging_root_table_name: str,
                          key_clauses: Sequence[str],
                          for_delete: bool) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/redshift/redshift.py#L201)

Generate sql clauses that may be used to select or delete rows in root table of destination dataset

A list of clauses may be returned for engines that do not support OR in subqueries. Like BigQuery

## RedshiftClient Objects

```python
class RedshiftClient(InsertValuesJobClient, SupportsStagingDestination)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/redshift/redshift.py#L223)

### create\_load\_job

```python
def create_load_job(table: TTableSchema,
                    file_path: str,
                    load_id: str,
                    restore: bool = False) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/redshift/redshift.py#L257)

Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs

