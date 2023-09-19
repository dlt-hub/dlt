---
sidebar_label: mssql
title: destinations.mssql.mssql
---

## MsSqlMergeJob Objects

```python
class MsSqlMergeJob(SqlMergeJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/mssql/mssql.py#L68)

#### gen\_key\_table\_clauses

```python
@classmethod
def gen_key_table_clauses(cls, root_table_name: str,
                          staging_root_table_name: str,
                          key_clauses: Sequence[str],
                          for_delete: bool) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/mssql/mssql.py#L70)

Generate sql clauses that may be used to select or delete rows in root table of destination dataset

