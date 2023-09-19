---
sidebar_label: sql_jobs
title: destinations.sql_jobs
---

## SqlBaseJob Objects

```python
class SqlBaseJob(NewLoadJobImpl)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/sql_jobs.py#L15)

Sql base job for jobs that rely on the whole tablechain

#### from\_table\_chain

```python
@classmethod
def from_table_chain(cls, table_chain: Sequence[TTableSchema],
                     sql_client: SqlClientBase[Any]) -> NewLoadJobImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/sql_jobs.py#L20)

Generates a list of sql statements, that will be executed by the sql client when the job is executed in the loader.

The `table_chain` contains a list schemas of a tables with parent-child relationship, ordered by the ancestry (the root of the tree is first on the list).

## SqlStagingCopyJob Objects

```python
class SqlStagingCopyJob(SqlBaseJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/sql_jobs.py#L46)

Generates a list of sql statements that copy the data from staging dataset into destination dataset.

## SqlMergeJob Objects

```python
class SqlMergeJob(SqlBaseJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/sql_jobs.py#L62)

Generates a list of sql statements that merge the data from staging dataset into destination dataset.

#### generate\_sql

```python
@classmethod
def generate_sql(cls, table_chain: Sequence[TTableSchema],
                 sql_client: SqlClientBase[Any]) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/sql_jobs.py#L67)

Generates a list of sql statements that merge the data in staging dataset with the data in destination dataset.

The `table_chain` contains a list schemas of a tables with parent-child relationship, ordered by the ancestry (the root of the tree is first on the list).
The root table is merged using primary_key and merge_key hints which can be compound and be both specified. In that case the OR clause is generated.
The child tables are merged based on propagated `root_key` which is a type of foreign key but always leading to a root table.

First we store the root_keys of root table elements to be deleted in the temp table. Then we use the temp table to delete records from root and all child tables in the destination dataset.
At the end we copy the data from the staging dataset into destination dataset.

#### gen\_key\_table\_clauses

```python
@classmethod
def gen_key_table_clauses(cls, root_table_name: str,
                          staging_root_table_name: str,
                          key_clauses: Sequence[str],
                          for_delete: bool) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/sql_jobs.py#L91)

Generate sql clauses that may be used to select or delete rows in root table of destination dataset

A list of clauses may be returned for engines that do not support OR in subqueries. Like BigQuery

#### gen\_delete\_temp\_table\_sql

```python
@classmethod
def gen_delete_temp_table_sql(
        cls, unique_column: str,
        key_table_clauses: Sequence[str]) -> Tuple[List[str], str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/sql_jobs.py#L99)

Generate sql that creates delete temp table and inserts `unique_column` from root table for all records to delete. May return several statements.

Returns temp table name for cases where special names are required like SQLServer.

