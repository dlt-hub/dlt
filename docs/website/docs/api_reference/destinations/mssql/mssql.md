---
sidebar_label: mssql
title: destinations.mssql.mssql
---

## MsSqlMergeJob Objects

```python
class MsSqlMergeJob(SqlMergeJob)
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

