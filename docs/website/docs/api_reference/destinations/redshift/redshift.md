---
sidebar_label: redshift
title: destinations.redshift.redshift
---

## RedshiftMergeJob Objects

```python
class RedshiftMergeJob(SqlMergeJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/redshift/redshift.py#L146)

#### gen\_key\_table\_clauses

```python
@classmethod
def gen_key_table_clauses(cls, root_table_name: str,
                          staging_root_table_name: str,
                          key_clauses: Sequence[str],
                          for_delete: bool) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/redshift/redshift.py#L149)

Generate sql clauses that may be used to select or delete rows in root table of destination dataset

A list of clauses may be returned for engines that do not support OR in subqueries. Like BigQuery

## RedshiftClient Objects

```python
class RedshiftClient(InsertValuesJobClient)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/redshift/redshift.py#L159)

#### start\_file\_load

```python
def start_file_load(table: TTableSchema, file_path: str,
                    load_id: str) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/redshift/redshift.py#L180)

Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs

