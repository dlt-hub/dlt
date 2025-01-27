---
sidebar_label: lancedb_client
title: destinations.impl.lancedb.lancedb_client
---

## write\_records

```python
def write_records(records: DATA,
                  *,
                  db_client: DBConnection,
                  table_name: str,
                  write_disposition: Optional[TWriteDisposition] = "append",
                  merge_key: Optional[str] = None,
                  remove_orphans: Optional[bool] = False,
                  filter_condition: Optional[str] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/lancedb_client.py#L169)

Inserts records into a LanceDB table with automatic embedding computation.

**Arguments**:

- `records` - The data to be inserted as payload.
- `db_client` - The LanceDB client connection.
- `table_name` - The name of the table to insert into.
- `merge_key` - Keys for update/merge operations.
- `write_disposition` - The write disposition - one of 'skip', 'append', 'replace', 'merge'.
- `remove_orphans` _bool_ - Whether to remove orphans after insertion or not (only merge disposition).
- `filter_condition` _str_ - If None, then all such rows will be deleted.
  Otherwise, the condition will be used as an SQL filter to limit what rows are deleted.
  

**Raises**:

- `ValueError` - If the write disposition is unsupported, or `id_field_name` is not
  provided for update/merge operations.

## LanceDBClient Objects

```python
class LanceDBClient(JobClientBase, WithStateSync)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/lancedb_client.py#L228)

LanceDB destination handler.

### model\_func

The embedder callback used for each chunk.

### create\_table

```python
@lancedb_error
def create_table(table_name: str,
                 schema: TArrowSchema,
                 mode: str = "create") -> "lancedb.table.Table"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/lancedb_client.py#L293)

Create a LanceDB Table from the provided LanceModel or PyArrow schema.

**Arguments**:

- `schema` - The table schema to create.
- `table_name` - The name of the table to create.
- `mode` _str_ - The mode to use when creating the table. Can be either "create" or "overwrite".
  By default, if the table already exists, an exception is raised.
  If you want to overwrite the table, use mode="overwrite".

### delete\_table

```python
def delete_table(table_name: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/lancedb_client.py#L307)

Delete a LanceDB table.

**Arguments**:

- `table_name` - The name of the table to delete.

### query\_table

```python
def query_table(
    table_name: str,
    query: Union[List[Any], NDArray, Array, ChunkedArray, str, Tuple[Any],
                 None] = None
) -> LanceQueryBuilder
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/lancedb_client.py#L315)

Query a LanceDB table.

**Arguments**:

- `table_name` - The name of the table to query.
- `query` - The targeted vector to search for.
  

**Returns**:

  A LanceDB query builder.

### drop\_storage

```python
@lancedb_error
def drop_storage() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/lancedb_client.py#L349)

Drop the dataset from the LanceDB instance.

Deletes all tables in the dataset and all data, as well as sentinel table associated with them.

If the dataset name wasn't provided, it deletes all the tables in the current schema.

### extend\_lancedb\_table\_schema

```python
@lancedb_error
def extend_lancedb_table_schema(table_name: str,
                                field_schemas: List[pa.Field]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/lancedb_client.py#L455)

Extend LanceDB table schema with empty columns.

**Arguments**:

- `table_name` - The name of the table to create the fields on.
- `field_schemas` - The list of PyArrow Fields to create in the target LanceDB table.

### get\_stored\_state

```python
@lancedb_error
def get_stored_state(pipeline_name: str) -> Optional[StateInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/lancedb_client.py#L562)

Retrieves the latest completed state for a pipeline.

### get\_stored\_schema

```python
@lancedb_error
def get_stored_schema(schema_name: str = None) -> Optional[StorageSchemaInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/lancedb_client.py#L647)

Retrieves newest schema from destination storage.

