---
sidebar_label: db_api_client
title: destinations.impl.sqlalchemy.db_api_client
---

## SqlalchemyClient Objects

```python
class SqlalchemyClient(SqlClientBase[Connection])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/sqlalchemy/db_api_client.py#L106)

### dbapi

type: ignore[assignment]

### migrations

lazy init as needed

### commit\_transaction

```python
def commit_transaction() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/sqlalchemy/db_api_client.py#L202)

Commits the current transaction.

### rollback\_transaction

```python
def rollback_transaction() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/sqlalchemy/db_api_client.py#L206)

Rolls back the current transaction.

### get\_existing\_table

```python
def get_existing_table(table_name: str) -> Optional[sa.Table]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/sqlalchemy/db_api_client.py#L339)

Get a table object from metadata if it exists

### compile\_column\_def

```python
def compile_column_def(column: sa.Column) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/sqlalchemy/db_api_client.py#L385)

Compile a column definition including type for ADD COLUMN clause

### reflect\_table

```python
def reflect_table(
        table_name: str,
        metadata: Optional[sa.MetaData] = None,
        include_columns: Optional[Sequence[str]] = None) -> Optional[sa.Table]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/sqlalchemy/db_api_client.py#L389)

Reflect a table from the database and return the Table object

### compare\_storage\_table

```python
def compare_storage_table(
        table_name: str) -> Tuple[sa.Table, List[sa.Column], bool]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/sqlalchemy/db_api_client.py#L411)

Reflect the table from database and compare it with the version already in metadata.
Returns a 3 part tuple:
- The current version of the table in metadata
- List of columns that are missing from the storage table (all columns if it doesn't exist in storage)
- boolean indicating whether the table exists in storage

