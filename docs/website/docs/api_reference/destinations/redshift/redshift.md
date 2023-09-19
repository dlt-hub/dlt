---
sidebar_label: redshift
title: destinations.redshift.redshift
---

## RedshiftMergeJob Objects

```python
class RedshiftMergeJob(SqlMergeJob)
```

#### gen\_key\_table\_clauses

```python
@classmethod
def gen_key_table_clauses(cls, root_table_name: str,
                          staging_root_table_name: str,
                          key_clauses: Sequence[str],
                          for_delete: bool) -> List[str]
```

Generate sql clauses that may be used to select or delete rows in root table of destination dataset

A list of clauses may be returned for engines that do not support OR in subqueries. Like BigQuery

## RedshiftClient Objects

```python
class RedshiftClient(InsertValuesJobClient)
```

#### start\_file\_load

```python
def start_file_load(table: TTableSchema, file_path: str,
                    load_id: str) -> LoadJob
```

Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs

