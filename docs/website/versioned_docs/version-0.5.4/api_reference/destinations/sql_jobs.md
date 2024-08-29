---
sidebar_label: sql_jobs
title: destinations.sql_jobs
---

## SqlBaseJob Objects

```python
class SqlBaseJob(NewLoadJobImpl)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_jobs.py#L27)

Sql base job for jobs that rely on the whole tablechain

### from\_table\_chain

```python
@classmethod
def from_table_chain(cls,
                     table_chain: Sequence[TTableSchema],
                     sql_client: SqlClientBase[Any],
                     params: Optional[SqlJobParams] = None) -> NewLoadJobImpl
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_jobs.py#L33)

Generates a list of sql statements, that will be executed by the sql client when the job is executed in the loader.

The `table_chain` contains a list schemas of a tables with parent-child relationship, ordered by the ancestry (the root of the tree is first on the list).

## SqlStagingCopyJob Objects

```python
class SqlStagingCopyJob(SqlBaseJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_jobs.py#L76)

Generates a list of sql statements that copy the data from staging dataset into destination dataset.

## SqlMergeJob Objects

```python
class SqlMergeJob(SqlBaseJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_jobs.py#L135)

Generates a list of sql statements that merge the data from staging dataset into destination dataset.

### generate\_sql

```python
@classmethod
def generate_sql(cls,
                 table_chain: Sequence[TTableSchema],
                 sql_client: SqlClientBase[Any],
                 params: Optional[SqlJobParams] = None) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_jobs.py#L141)

Generates a list of sql statements that merge the data in staging dataset with the data in destination dataset.

The `table_chain` contains a list schemas of a tables with parent-child relationship, ordered by the ancestry (the root of the tree is first on the list).
The root table is merged using primary_key and merge_key hints which can be compound and be both specified. In that case the OR clause is generated.
The child tables are merged based on propagated `root_key` which is a type of foreign key but always leading to a root table.

First we store the root_keys of root table elements to be deleted in the temp table. Then we use the temp table to delete records from root and all child tables in the destination dataset.
At the end we copy the data from the staging dataset into destination dataset.

If a hard_delete column is specified, records flagged as deleted will be excluded from the copy into the destination dataset.
If a dedup_sort column is specified in conjunction with a primary key, records will be sorted before deduplication, so the "latest" record remains.

### gen\_key\_table\_clauses

```python
@classmethod
def gen_key_table_clauses(cls, root_table_name: str,
                          staging_root_table_name: str,
                          key_clauses: Sequence[str],
                          for_delete: bool) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_jobs.py#L179)

Generate sql clauses that may be used to select or delete rows in root table of destination dataset

A list of clauses may be returned for engines that do not support OR in subqueries. Like BigQuery

### gen\_delete\_temp\_table\_sql

```python
@classmethod
def gen_delete_temp_table_sql(
        cls, unique_column: str,
        key_table_clauses: Sequence[str]) -> Tuple[List[str], str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_jobs.py#L196)

Generate sql that creates delete temp table and inserts `unique_column` from root table for all records to delete. May return several statements.

Returns temp table name for cases where special names are required like SQLServer.

### gen\_select\_from\_dedup\_sql

```python
@classmethod
def gen_select_from_dedup_sql(cls,
                              table_name: str,
                              primary_keys: Sequence[str],
                              columns: Sequence[str],
                              dedup_sort: Tuple[str, TSortOrder] = None,
                              condition: str = None,
                              condition_columns: Sequence[str] = None) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_jobs.py#L212)

Returns SELECT FROM SQL statement.

The FROM clause in the SQL statement represents a deduplicated version
of the `table_name` table.

Expects column names provided in arguments to be escaped identifiers.

**Arguments**:

- `table_name` - Name of the table that is selected from.
- `primary_keys` - A sequence of column names representing the primary
  key of the table. Is used to deduplicate the table.
- `columns` - Sequence of column names that will be selected from
  the table.
- `sort_column` - Name of a column to sort the records by within a
  primary key. Values in the column are sorted in descending order,
  so the record with the highest value in `sort_column` remains
  after deduplication. No sorting is done if a None value is provided,
  leading to arbitrary deduplication.
- `condition` - String used as a WHERE clause in the SQL statement to
  filter records. The name of any column that is used in the
  condition but is not part of `columns` must be provided in the
  `condition_columns` argument. No filtering is done (aside from the
  deduplication) if a None value is provided.
- `condition_columns` - Sequence of names of columns used in the `condition`
  argument. These column names will be selected in the inner subquery
  to make them accessible to the outer WHERE clause. This argument
  should only be used in combination with the `condition` argument.
  

**Returns**:

  A string representing a SELECT FROM SQL statement where the FROM
  clause represents a deduplicated version of the `table_name` table.
  
  The returned value is used in two ways:
  1) To select the values for an INSERT INTO statement.
  2) To select the values for a temporary table used for inserts.

### gen\_delete\_from\_sql

```python
@classmethod
def gen_delete_from_sql(cls, table_name: str, unique_column: str,
                        delete_temp_table_name: str,
                        temp_table_column: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_jobs.py#L301)

Generate DELETE FROM statement deleting the records found in the deletes temp table.

