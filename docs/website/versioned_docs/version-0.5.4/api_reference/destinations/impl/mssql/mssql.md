---
sidebar_label: mssql
title: destinations.impl.mssql.mssql
---

## MsSqlMergeJob Objects

```python
class MsSqlMergeJob(SqlMergeFollowupJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/mssql/mssql.py#L113)

### gen\_key\_table\_clauses

```python
@classmethod
def gen_key_table_clauses(cls, root_table_name: str,
                          staging_root_table_name: str,
                          key_clauses: Sequence[str],
                          for_delete: bool) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/mssql/mssql.py#L115)

Generate sql clauses that may be used to select or delete rows in root table of destination dataset

