---
sidebar_label: mssql
title: destinations.impl.mssql.mssql
---

## MsSqlMergeJob Objects

```python
class MsSqlMergeJob(SqlMergeFollowupJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/mssql/mssql.py#L50)

### gen\_key\_table\_clauses

```python
@classmethod
def gen_key_table_clauses(cls, root_table_name: str,
                          staging_root_table_name: str,
                          key_clauses: Sequence[str],
                          for_delete: bool) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/mssql/mssql.py#L52)

Generate sql clauses that may be used to select or delete rows in root table of destination dataset

