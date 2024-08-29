---
sidebar_label: schema
title: common.schema.schema
---

## Schema Objects

```python
class Schema()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L62)

### naming

Naming convention used by the schema to normalize identifiers

### data\_item\_normalizer

Data item normalizer used by the schema to create tables

### version\_table\_name

Normalized name of the version table

### loads\_table\_name

Normalized name of the loads table

### state\_table\_name

Normalized name of the dlt state table

### replace\_schema\_content

```python
def replace_schema_content(schema: "Schema",
                           link_to_replaced_schema: bool = False) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L129)

Replaces content of the current schema with `schema` content. Does not compute new schema hash and
does not increase the numeric version. Optionally will link the replaced schema to incoming schema
by keeping its hash in prev hashes and setting stored hash to replaced schema hash.

### coerce\_row

```python
def coerce_row(table_name: str, parent_table: str,
               row: StrAny) -> Tuple[DictStrAny, TPartialTableSchema]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L222)

Fits values of fields present in `row` into a schema of `table_name`. Will coerce values into data types and infer new tables and column schemas.

Method expects that field names in row are already normalized.
* if table schema for `table_name` does not exist, new table is created
* if column schema for a field in `row` does not exist, it is inferred from data
* if incomplete column schema (no data type) exists, column is inferred from data and existing hints are applied
* fields with None value are removed

Returns tuple with row with coerced values and a partial table containing just the newly added columns or None if no changes were detected

### apply\_schema\_contract

```python
def apply_schema_contract(
    schema_contract: TSchemaContractDict,
    partial_table: TPartialTableSchema,
    data_item: TDataItem = None,
    raise_on_freeze: bool = True
) -> Tuple[TPartialTableSchema, List[Tuple[TSchemaContractEntities, str,
                                           TSchemaEvolutionMode]]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L262)

Checks if `schema_contract` allows for the `partial_table` to update the schema. It applies the contract dropping
the affected columns or the whole `partial_table`. It generates and returns a set of filters that should be applied to incoming data in order to modify it
so it conforms to the contract. `data_item` is provided only as evidence in case DataValidationError is raised.

Example `schema_contract`:
{
"tables": "freeze",
"columns": "evolve",
"data_type": "discard_row"
}

Settings for table affects new tables, settings for column affects new columns and settings for data_type affects new variant columns. Each setting can be set to one of:
* evolve: allow all changes
* freeze: allow no change and fail the load
* discard_row: allow no schema change and filter out the row
* discard_value: allow no schema change and filter out the value but load the rest of the row

Returns a tuple where a first element is modified partial table and the second is a list of filters. The modified partial may be None in case the
whole table is not allowed.
Each filter is a tuple of (table|columns, entity name, freeze | discard_row | discard_value).
Note: by default `freeze` immediately raises DataValidationError which is convenient in most use cases

### expand\_schema\_contract\_settings

```python
@staticmethod
def expand_schema_contract_settings(
        settings: TSchemaContract,
        default: TSchemaContractDict = None) -> TSchemaContractDict
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L377)

Expand partial or shorthand settings into full settings dictionary using `default` for unset entities

### resolve\_contract\_settings\_for\_table

```python
def resolve_contract_settings_for_table(
        table_name: str,
        new_table_schema: TTableSchema = None) -> TSchemaContractDict
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L387)

Resolve the exact applicable schema contract settings for the table `table_name`. `new_table_schema` is added to the tree during the resolution.

### update\_table

```python
def update_table(partial_table: TPartialTableSchema) -> TPartialTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L409)

Adds or merges `partial_table` into the schema. Identifiers are not normalized

### update\_schema

```python
def update_schema(schema: "Schema") -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L434)

Updates this schema from an incoming schema. Normalizes identifiers after updating normalizers.

### bump\_version

```python
def bump_version() -> Tuple[int, str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L445)

Computes schema hash in order to check if schema content was modified. In such case the schema ``stored_version`` and ``stored_version_hash`` are updated.

Should not be used in production code. The method ``to_dict`` will generate TStoredSchema with correct value, only once before persisting schema to storage.

**Returns**:

  Tuple[int, str]: Current (``stored_version``, ``stored_version_hash``) tuple

### normalize\_table\_identifiers

```python
def normalize_table_identifiers(table: TTableSchema) -> TTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L495)

Normalizes all table and column names in `table` schema according to current schema naming convention and returns
new normalized TTableSchema instance.

Naming convention like snake_case may produce name clashes with the column names. Clashing column schemas are merged
where the column that is defined later in the dictionary overrides earlier column.

Note that resource name is not normalized.

### get\_new\_table\_columns

```python
def get_new_table_columns(
        table_name: str,
        exiting_columns: TTableSchemaColumns,
        include_incomplete: bool = False) -> List[TColumnSchema]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L526)

Gets new columns to be added to `exiting_columns` to bring them up to date with `table_name` schema. Optionally includes incomplete columns (without data type)

### get\_table\_columns

```python
def get_table_columns(table_name: str,
                      include_incomplete: bool = False) -> TTableSchemaColumns
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L543)

Gets columns of `table_name`. Optionally includes incomplete columns

### data\_tables

```python
def data_tables(include_incomplete: bool = False) -> List[TTableSchema]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L556)

Gets list of all tables, that hold the loaded data. Excludes dlt tables. Excludes incomplete tables (ie. without columns)

### data\_table\_names

```python
def data_table_names() -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L567)

Returns list of table table names. Excludes dlt table names.

### dlt\_tables

```python
def dlt_tables() -> List[TTableSchema]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L571)

Gets dlt tables

### dlt\_table\_names

```python
def dlt_table_names() -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L577)

Returns list of dlt table names.

### is\_new\_table

```python
def is_new_table(table_name: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L584)

Returns true if this table does not exist OR is incomplete (has only incomplete columns) and therefore new

### version

```python
@property
def version() -> int
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L595)

Version of the schema content that takes into account changes from the time of schema loading/creation.
The stored version is increased by one if content was modified

**Returns**:

- `int` - Current schema version

### stored\_version

```python
@property
def stored_version() -> int
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L605)

Version of the schema content form the time of schema loading/creation.

**Returns**:

- `int` - Stored schema version

### version\_hash

```python
@property
def version_hash() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L614)

Current version hash of the schema, recomputed from the actual content

### previous\_hashes

```python
@property
def previous_hashes() -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L619)

Current version hash of the schema, recomputed from the actual content

### stored\_version\_hash

```python
@property
def stored_version_hash() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L624)

Version hash of the schema content form the time of schema loading/creation.

### tables

```python
@property
def tables() -> TSchemaTables
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L633)

Dictionary of schema tables

### clone

```python
def clone(with_name: str = None, update_normalizers: bool = False) -> "Schema"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L649)

Make a deep copy of the schema, optionally changing the name, and updating normalizers and identifiers in the schema if `update_normalizers` is True

Note that changing of name will break the previous version chain

### update\_normalizers

```python
def update_normalizers() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L664)

Looks for new normalizer configuration or for destination capabilities context and updates all identifiers in the schema

### add\_type\_detection

```python
def add_type_detection(detection: TTypeDetections) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L678)

Add type auto detection to the schema.

### remove\_type\_detection

```python
def remove_type_detection(detection: TTypeDetections) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/schema.py#L684)

Adds type auto detection to the schema.

