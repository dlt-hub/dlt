---
sidebar_label: pyarrow
title: common.libs.pyarrow
---

## remove\_null\_columns

```python
def remove_null_columns(item: TAnyArrowItem) -> TAnyArrowItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/libs/pyarrow.py#L154)

Remove all columns of datatype pyarrow.null() from the table or record batch

## remove\_columns

```python
def remove_columns(item: TAnyArrowItem,
                   columns: Sequence[str]) -> TAnyArrowItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/libs/pyarrow.py#L161)

Remove `columns` from Arrow `item`

## append\_column

```python
def append_column(item: TAnyArrowItem, name: str, data: Any) -> TAnyArrowItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/libs/pyarrow.py#L175)

Appends new column to Table or RecordBatch

## rename\_columns

```python
def rename_columns(item: TAnyArrowItem,
                   new_column_names: Sequence[str]) -> TAnyArrowItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/libs/pyarrow.py#L188)

Rename arrow columns on Table or RecordBatch, returns same data but with renamed schema

## normalize\_py\_arrow\_schema

```python
def normalize_py_arrow_schema(
        item: TAnyArrowItem, columns: TTableSchemaColumns,
        naming: NamingConvention,
        caps: DestinationCapabilitiesContext) -> TAnyArrowItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/libs/pyarrow.py#L206)

Normalize arrow `item` schema according to the `columns`.

1. arrow schema field names will be normalized according to `naming`
2. arrows columns will be reordered according to `columns`
3. empty columns will be inserted if they are missing, types will be generated using `caps`

## get\_normalized\_arrow\_fields\_mapping

```python
def get_normalized_arrow_fields_mapping(item: TAnyArrowItem,
                                        naming: NamingConvention) -> StrStr
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/libs/pyarrow.py#L269)

Normalizes schema field names and returns mapping from original to normalized name. Raises on name clashes

## py\_arrow\_to\_table\_schema\_columns

```python
def py_arrow_to_table_schema_columns(
        schema: pyarrow.Schema) -> TTableSchemaColumns
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/libs/pyarrow.py#L283)

Convert a PyArrow schema to a table schema columns dict.

**Arguments**:

- `schema` _pyarrow.Schema_ - pyarrow schema
  

**Returns**:

- `TTableSchemaColumns` - table schema columns

## get\_row\_count

```python
def get_row_count(parquet_file: TFileOrPath) -> int
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/libs/pyarrow.py#L302)

Get the number of rows in a parquet file.

**Arguments**:

- `parquet_file` _str_ - path to parquet file
  

**Returns**:

- `int` - number of rows

## to\_arrow\_scalar

```python
def to_arrow_scalar(value: Any, arrow_type: pyarrow.DataType) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/libs/pyarrow.py#L319)

Converts python value to an arrow compute friendly version

## from\_arrow\_scalar

```python
def from_arrow_scalar(arrow_value: pyarrow.Scalar) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/libs/pyarrow.py#L324)

Converts arrow scalar into Python type. Currently adds "UTC" to naive date times and converts all others to UTC

## TNewColumns

Sequence of tuples: (field index, field, generating function)

## pq\_stream\_with\_new\_columns

```python
def pq_stream_with_new_columns(
        parquet_file: TFileOrPath,
        columns: TNewColumns,
        row_groups_per_read: int = 1) -> Iterator[pyarrow.Table]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/libs/pyarrow.py#L341)

Add column(s) to the table in batches.

The table is read from parquet `row_groups_per_read` row groups at a time

**Arguments**:

- `parquet_file` - path or file object to parquet file
- `columns` - list of columns to add in the form of (insertion index, `pyarrow.Field`, column_value_callback)
  The callback should accept a `pyarrow.Table` and return an array of values for the column.
- `row_groups_per_read` - number of row groups to read at a time. Defaults to 1.
  

**Yields**:

  `pyarrow.Table` objects with the new columns added.

