---
sidebar_label: lancedb_client
title: destinations.impl.lancedb.lancedb_client
---

## upload\_batch

```python
def upload_batch(records: List[DictStrAny],
                 *,
                 db_client: DBConnection,
                 table_name: str,
                 write_disposition: TWriteDisposition,
                 id_field_name: Optional[str] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/lancedb_client.py#L152)

Inserts records into a LanceDB table with automatic embedding computation.

**Arguments**:

- `records` - The data to be inserted as payload.
- `db_client` - The LanceDB client connection.
- `table_name` - The name of the table to insert into.
- `id_field_name` - The name of the ID field for update/merge operations.
- `write_disposition` - The write disposition - one of 'skip', 'append', 'replace', 'merge'.
  

**Raises**:

- `ValueError` - If the write disposition is unsupported, or `id_field_name` is not
  provided for update/merge operations.

## LanceDBClient Objects

```python
class LanceDBClient(JobClientBase, WithStateSync)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/lancedb_client.py#L205)

LanceDB destination handler.

### create\_table

```python
@lancedb_error
def create_table(table_name: str,
                 schema: TArrowSchema,
                 mode: str = "create") -> Table
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/lancedb_client.py#L279)

Create a LanceDB Table from the provided LanceModel or PyArrow schema.

**Arguments**:

- `schema` - The table schema to create.
- `table_name` - The name of the table to create.
  mode (): The mode to use when creating the table. Can be either "create" or "overwrite".
  By default, if the table already exists, an exception is raised.
  If you want to overwrite the table, use mode="overwrite".

### delete\_table

```python
def delete_table(table_name: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/lancedb_client.py#L291)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/lancedb_client.py#L299)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/lancedb_client.py#L333)

Drop the dataset from the LanceDB instance.

Deletes all tables in the dataset and all data, as well as sentinel table associated with them.

If the dataset name was not provided, it deletes all the tables in the current schema.

### add\_table\_fields

```python
@lancedb_error
def add_table_fields(table_name: str,
                     field_schemas: List[TArrowField]) -> Optional[Table]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/lancedb_client.py#L423)

Add multiple fields to the LanceDB table at once.

**Arguments**:

- `table_name` - The name of the table to create the fields on.
- `field_schemas` - The list of fields to create.

### get\_stored\_state

```python
@lancedb_error
def get_stored_state(pipeline_name: str) -> Optional[StateInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/lancedb_client.py#L536)

Retrieves the latest completed state for a pipeline.

### get\_stored\_schema

```python
@lancedb_error
def get_stored_schema() -> Optional[StorageSchemaInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/lancedb_client.py#L621)

Retrieves newest schema from destination storage.

