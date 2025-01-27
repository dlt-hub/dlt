---
sidebar_label: pyarrow
title: common.libs.pyarrow
---

## get\_column\_type\_from\_py\_arrow

```python
def get_column_type_from_py_arrow(dtype: pyarrow.DataType) -> TColumnType
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L125)

Returns (data_type, precision, scale) tuple from pyarrow.DataType

## remove\_null\_columns

```python
def remove_null_columns(item: TAnyArrowItem) -> TAnyArrowItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L173)

Remove all columns of datatype pyarrow.null() from the table or record batch

## remove\_columns

```python
def remove_columns(item: TAnyArrowItem,
                   columns: Sequence[str]) -> TAnyArrowItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L180)

Remove `columns` from Arrow `item`

## append\_column

```python
def append_column(item: TAnyArrowItem, name: str, data: Any) -> TAnyArrowItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L194)

Appends new column to Table or RecordBatch

## rename\_columns

```python
def rename_columns(item: TAnyArrowItem,
                   new_column_names: Sequence[str]) -> TAnyArrowItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L207)

Rename arrow columns on Table or RecordBatch, returns same data but with renamed schema

## normalize\_py\_arrow\_item

```python
def normalize_py_arrow_item(item: TAnyArrowItem,
                            columns: TTableSchemaColumns,
                            naming: NamingConvention,
                            caps: DestinationCapabilitiesContext,
                            load_id: Optional[str] = None) -> TAnyArrowItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L280)

Normalize arrow `item` schema according to the `columns`.

1. arrow schema field names will be normalized according to `naming`
2. arrows columns will be reordered according to `columns`
3. empty columns will be inserted if they are missing, types will be generated using `caps`
4. arrow columns with different nullability than corresponding schema columns will be updated
5. Add `_dlt_load_id` column if it is missing and `load_id` is provided

## get\_normalized\_arrow\_fields\_mapping

```python
def get_normalized_arrow_fields_mapping(schema: pyarrow.Schema,
                                        naming: NamingConvention) -> StrStr
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L350)

Normalizes schema field names and returns mapping from original to normalized name. Raises on name collisions

## py\_arrow\_to\_table\_schema\_columns

```python
def py_arrow_to_table_schema_columns(
        schema: pyarrow.Schema) -> TTableSchemaColumns
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L364)

Convert a PyArrow schema to a table schema columns dict.

**Arguments**:

- `schema` _pyarrow.Schema_ - pyarrow schema
  

**Returns**:

- `TTableSchemaColumns` - table schema columns

## get\_parquet\_metadata

```python
def get_parquet_metadata(
        parquet_file: TFileOrPath) -> Tuple[int, pyarrow.Schema]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L383)

Gets parquet file metadata (including row count and schema)

**Arguments**:

- `parquet_file` _str_ - path to parquet file
  

**Returns**:

- `FileMetaData` - file metadata

## to\_arrow\_scalar

```python
def to_arrow_scalar(value: Any, arrow_type: pyarrow.DataType) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L400)

Converts python value to an arrow compute friendly version

## from\_arrow\_scalar

```python
def from_arrow_scalar(arrow_value: pyarrow.Scalar) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L405)

Converts arrow scalar into Python type. Currently adds "UTC" to naive date times and converts all others to UTC

## TNewColumns

Sequence of tuples: (field index, field, generating function)

## add\_constant\_column

```python
def add_constant_column(item: TAnyArrowItem,
                        name: str,
                        data_type: pyarrow.DataType,
                        value: Any = None,
                        nullable: bool = True,
                        index: int = -1) -> TAnyArrowItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L422)

Add column with a single value to the table.

**Arguments**:

- `item` - Arrow table or record batch
- `name` - The new column name
- `data_type` - The data type of the new column
- `nullable` - Whether the new column is nullable
- `value` - The value to fill the new column with
- `index` - The index at which to insert the new column. Defaults to -1 (append)

## pq\_stream\_with\_new\_columns

```python
def pq_stream_with_new_columns(
        parquet_file: TFileOrPath,
        columns: TNewColumns,
        row_groups_per_read: int = 1) -> Iterator[pyarrow.Table]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L446)

Add column(s) to the table in batches.

The table is read from parquet `row_groups_per_read` row groups at a time

**Arguments**:

- `parquet_file` - path or file object to parquet file
- `columns` - list of columns to add in the form of (insertion index, `pyarrow.Field`, column_value_callback)
  The callback should accept a `pyarrow.Table` and return an array of values for the column.
- `row_groups_per_read` - number of row groups to read at a time. Defaults to 1.
  

**Yields**:

  `pyarrow.Table` objects with the new columns added.

## cast\_arrow\_schema\_types

```python
def cast_arrow_schema_types(
    schema: pyarrow.Schema, type_map: Dict[Callable[[pyarrow.DataType], bool],
                                           Callable[..., pyarrow.DataType]]
) -> pyarrow.Schema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/libs/pyarrow.py#L477)

Returns type-casted Arrow schema.

Replaces data types for fields matching a type check in `type_map`.
Type check functions in `type_map` are assumed to be mutually exclusive, i.e.
a data type does not match more than one type check function.

