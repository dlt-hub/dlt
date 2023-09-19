---
sidebar_label: schema
title: common.schema.schema
---

## Schema Objects

```python
class Schema()
```

#### naming

Naming convention used by the schema to normalize identifiers

#### data\_item\_normalizer

Data item normalizer used by the schema to create tables

#### version\_table\_name

Normalized name of the version table

#### loads\_table\_name

Normalized name of the loads table

#### state\_table\_name

Normalized name of the dlt state table

#### coerce\_row

```python
def coerce_row(table_name: str, parent_table: str,
               row: StrAny) -> Tuple[DictStrAny, TPartialTableSchema]
```

Fits values of fields present in `row` into a schema of `table_name`. Will coerce values into data types and infer new tables and column schemas.

Method expects that field names in row are already normalized.
* if table schema for `table_name` does not exist, new table is created
* if column schema for a field in `row` does not exist, it is inferred from data
* if incomplete column schema (no data type) exists, column is inferred from data and existing hints are applied
* fields with None value are removed

Returns tuple with row with coerced values and a partial table containing just the newly added columns or None if no changes were detected

#### bump\_version

```python
def bump_version() -> Tuple[int, str]
```

Computes schema hash in order to check if schema content was modified. In such case the schema ``stored_version`` and ``stored_version_hash`` are updated.

Should not be used in production code. The method ``to_dict`` will generate TStoredSchema with correct value, only once before persisting schema to storage.

**Returns**:

  Tuple[int, str]: Current (``stored_version``, ``stored_version_hash``) tuple

#### normalize\_table\_identifiers

```python
def normalize_table_identifiers(table: TTableSchema) -> TTableSchema
```

Normalizes all table and column names in `table` schema according to current schema naming convention and returns
new normalized TTableSchema instance.

Naming convention like snake_case may produce name clashes with the column names. Clashing column schemas are merged
where the column that is defined later in the dictionary overrides earlier column.

Note that resource name is not normalized.

#### get\_new\_table\_columns

```python
def get_new_table_columns(
        table_name: str,
        exiting_columns: TTableSchemaColumns,
        include_incomplete: bool = False) -> List[TColumnSchema]
```

Gets new columns to be added to `exiting_columns` to bring them up to date with `table_name` schema. Optionally includes incomplete columns (without data type)

#### get\_table\_columns

```python
def get_table_columns(table_name: str,
                      include_incomplete: bool = False) -> TTableSchemaColumns
```

Gets columns of `table_name`. Optionally includes incomplete columns

#### data\_tables

```python
def data_tables(include_incomplete: bool = False) -> List[TTableSchema]
```

Gets list of all tables, that hold the loaded data. Excludes dlt tables. Excludes incomplete tables (ie. without columns)

#### dlt\_tables

```python
def dlt_tables() -> List[TTableSchema]
```

Gets dlt tables

#### version

```python
@property
def version() -> int
```

Version of the schema content that takes into account changes from the time of schema loading/creation.
The stored version is increased by one if content was modified

**Returns**:

- `int` - Current schema version

#### stored\_version

```python
@property
def stored_version() -> int
```

Version of the schema content form the time of schema loading/creation.

**Returns**:

- `int` - Stored schema version

#### version\_hash

```python
@property
def version_hash() -> str
```

Current version hash of the schema, recomputed from the actual content

#### stored\_version\_hash

```python
@property
def stored_version_hash() -> str
```

Version hash of the schema content form the time of schema loading/creation.

#### tables

```python
@property
def tables() -> TSchemaTables
```

Dictionary of schema tables

#### clone

```python
def clone(update_normalizers: bool = False) -> "Schema"
```

Make a deep copy of the schema, possibly updating normalizers and identifiers in the schema if `update_normalizers` is True

#### update\_normalizers

```python
def update_normalizers() -> None
```

Looks for new normalizer configuration or for destination capabilities context and updates all identifiers in the schema

