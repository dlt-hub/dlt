---
sidebar_label: mssql
title: destinations.impl.mssql.mssql
---

## MsSqlMergeJob Objects

```python
class MsSqlMergeJob(SqlMergeJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/mssql/mssql.py#L112)

### gen\_key\_table\_clauses

```python
@classmethod
def gen_key_table_clauses(cls, root_table_name: str,
                          staging_root_table_name: str,
                          key_clauses: Sequence[str],
                          for_delete: bool) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/mssql/mssql.py#L114)

Generate sql clauses that may be used to select or delete rows in root table of destination dataset

