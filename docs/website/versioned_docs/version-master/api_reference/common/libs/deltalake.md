---
sidebar_label: deltalake
title: common.libs.deltalake
---

## ensure\_delta\_compatible\_arrow\_schema

```python
def ensure_delta_compatible_arrow_schema(
        schema: pa.Schema,
        partition_by: Optional[Union[List[str], str]] = None) -> pa.Schema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/deltalake.py#L27)

Returns Arrow schema compatible with Delta table format.

Casts schema to replace data types not supported by Delta.

## ensure\_delta\_compatible\_arrow\_data

```python
def ensure_delta_compatible_arrow_data(
    data: Union[pa.Table, pa.RecordBatchReader],
    partition_by: Optional[Union[List[str], str]] = None
) -> Union[pa.Table, pa.RecordBatchReader]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/deltalake.py#L56)

Returns Arrow data compatible with Delta table format.

Casts `data` schema to replace data types not supported by Delta.

## get\_delta\_write\_mode

```python
def get_delta_write_mode(write_disposition: TWriteDisposition) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/deltalake.py#L75)

Translates dlt write disposition to Delta write mode.

## write\_delta\_table

```python
def write_delta_table(
        table_or_uri: Union[str, Path, DeltaTable],
        data: Union[pa.Table, pa.RecordBatchReader],
        write_disposition: TWriteDisposition,
        partition_by: Optional[Union[List[str], str]] = None,
        storage_options: Optional[Dict[str, str]] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/deltalake.py#L88)

Writes in-memory Arrow data to on-disk Delta table.

Thin wrapper around `deltalake.write_deltalake`.

## merge\_delta\_table

```python
def merge_delta_table(table: DeltaTable, data: Union[pa.Table,
                                                     pa.RecordBatchReader],
                      schema: TTableSchema) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/deltalake.py#L114)

Merges in-memory Arrow data into on-disk Delta table.

## get\_delta\_tables

```python
def get_delta_tables(pipeline: Pipeline,
                     *tables: str,
                     schema_name: str = None) -> Dict[str, DeltaTable]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/deltalake.py#L151)

Returns Delta tables in `pipeline.default_schema (default)` as `deltalake.DeltaTable` objects.

Returned object is a dictionary with table names as keys and `DeltaTable` objects as values.
Optionally filters dictionary by table names specified as `*tables*`.
Raises ValueError if table name specified as `*tables` is not found. You may try to switch to other
schemas via `schema_name` argument.

