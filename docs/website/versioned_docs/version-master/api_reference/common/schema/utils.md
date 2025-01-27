---
sidebar_label: utils
title: common.schema.utils
---

## is\_valid\_schema\_name

```python
def is_valid_schema_name(name: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L64)

Schema name must be a valid python identifier and have max len of 64

## is\_nested\_table

```python
def is_nested_table(table: TTableSchema) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L73)

Checks if table is a dlt nested table: connected to parent table via row_key - parent_key reference

## normalize\_schema\_name

```python
def normalize_schema_name(name: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L79)

Normalizes schema name by using snake case naming convention. The maximum length is 64 characters

## apply\_defaults

```python
def apply_defaults(stored_schema: TStoredSchema) -> TStoredSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L85)

Applies default hint values to `stored_schema` in place

Updates only complete column hints, incomplete columns are preserved intact

## remove\_defaults

```python
def remove_defaults(stored_schema: TStoredSchema) -> TStoredSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L109)

Removes default values from `stored_schema` in place, returns the input for chaining

* removes column and table names from the value
* removed resource name if same as table name

## has\_default\_column\_prop\_value

```python
def has_default_column_prop_value(prop: str, value: Any) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L133)

Checks if `value` is a default for `prop`.

## remove\_column\_defaults

```python
def remove_column_defaults(column_schema: TColumnSchema) -> TColumnSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L142)

Removes default values from `column_schema` in place, returns the input for chaining

## bump\_version\_if\_modified

```python
def bump_version_if_modified(
        stored_schema: TStoredSchema) -> Tuple[int, str, str, Sequence[str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L151)

Bumps the `stored_schema` version and version hash if content modified, returns (new version, new hash, old hash, 10 last hashes) tuple

## normalize\_simple\_regex\_column

```python
def normalize_simple_regex_column(naming: NamingConvention,
                                  regex: TSimpleRegex) -> TSimpleRegex
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L213)

Assumes that regex applies to column name and normalizes it.

## compile\_simple\_regexes

```python
def compile_simple_regexes(r: Iterable[TSimpleRegex]) -> REPattern
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L319)

Compile multiple patterns as one

## is\_complete\_column

```python
def is_complete_column(col: TColumnSchemaBase) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L353)

Returns true if column contains enough data to be created at the destination. Must contain a name and a data type. Other hints have defaults.

## is\_nullable\_column

```python
def is_nullable_column(col: TColumnSchemaBase) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L358)

Returns true if column is nullable

## find\_incomplete\_columns

```python
def find_incomplete_columns(
        table: TTableSchema) -> Iterable[Tuple[TColumnSchemaBase, bool]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L363)

Yields (column, nullable) for all incomplete columns in `table`

## compare\_complete\_columns

```python
def compare_complete_columns(a: TColumnSchema, b: TColumnSchema) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L370)

Compares mandatory fields of complete columns

## diff\_table\_references

```python
def diff_table_references(
        a: Sequence[TTableReference],
        b: Sequence[TTableReference]) -> List[TTableReference]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L385)

Return a list of references containing references matched by table:
* References from `b` that are not in `a`
* References from `b` that are different from the one in `a`

## merge\_column

```python
def merge_column(col_a: TColumnSchema,
                 col_b: TColumnSchema,
                 merge_defaults: bool = True) -> TColumnSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L403)

Merges `col_b` into `col_a`. if `merge_defaults` is True, only hints from `col_b` that are not default in `col_a` will be set.

Modifies col_a in place and returns it

## merge\_columns

```python
def merge_columns(columns_a: TTableSchemaColumns,
                  columns_b: TTableSchemaColumns,
                  merge_columns: bool = False,
                  columns_partial: bool = True) -> TTableSchemaColumns
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L417)

Merges `columns_a` with `columns_b`. `columns_a` is modified in place.

* new columns are added
* if `merge_columns` is False, updated columns are replaced from `columns_b`
* if `merge_columns` is True, updated columns are merged with `merge_column`
* if `columns_partial` is True, both columns sets are considered incomplete. In that case hints like `primary_key` or `merge_key` are merged
* if `columns_partial` is False, hints like `primary_key` and `merge_key` are dropped from `columns_a` and replaced from `columns_b`
* incomplete columns in `columns_a` that got completed in `columns_b` are removed to preserve order

## diff\_table

```python
def diff_table(schema_name: str, tab_a: TTableSchema,
               tab_b: TPartialTableSchema) -> TPartialTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L448)

Creates a partial table that contains properties found in `tab_b` that are not present or different in `tab_a`.
The name is always present in returned partial.
It returns new columns (not present in tab_a) and merges columns from tab_b into tab_a (overriding non-default hint values).
If any columns are returned they contain full data (not diffs of columns)

Raises SchemaException if tables cannot be merged
* when columns with the same name have different data types
* when table links to different parent tables

## merge\_table

```python
def merge_table(schema_name: str, table: TTableSchema,
                partial_table: TPartialTableSchema) -> TPartialTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L534)

Merges "partial_table" into "table". `table` is merged in place. Returns the diff partial table.
`table` and `partial_table` names must be identical. A table diff is generated and applied to `table`

## merge\_diff

```python
def merge_diff(table: TTableSchema,
               table_diff: TPartialTableSchema) -> TPartialTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L543)

Merges a table diff `table_diff` into `table`. `table` is merged in place. Returns the diff.
* new columns are added, updated columns are replaced from diff
* incomplete columns in `table` that got completed in `partial_table` are removed to preserve order
* table hints are added or replaced from diff
* nothing gets deleted

## normalize\_table\_identifiers

```python
def normalize_table_identifiers(table: TTableSchema,
                                naming: NamingConvention) -> TTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L569)

Normalizes all table and column names in `table` schema according to current schema naming convention and returns
new instance with modified table schema.

Naming convention like snake_case may produce name collisions with the column names. Colliding column schemas are merged
where the column that is defined later in the dictionary overrides earlier column.

Note that resource name is not normalized.

## has\_table\_seen\_data

```python
def has_table_seen_data(table: TTableSchema) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L626)

Checks if normalizer has seen data coming to the table.

## remove\_processing\_hints

```python
def remove_processing_hints(tables: TSchemaTables) -> TSchemaTables
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L631)

Removes processing hints like x-normalizer and x-loader from schema tables. Modifies the input tables and returns it for convenience

## get\_processing\_hints

```python
def get_processing_hints(tables: TSchemaTables) -> Dict[str, List[str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L639)

Finds processing hints in a set of tables and returns table_name: [hints] mapping

## get\_first\_column\_name\_with\_prop

```python
def get_first_column_name_with_prop(
        table: TTableSchema,
        column_prop: Union[TColumnProp, str],
        include_incomplete: bool = False) -> Optional[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L668)

Returns name of first column in `table` schema with property `column_prop` or None if no such column exists.

## has\_column\_with\_prop

```python
def has_column_with_prop(table: TTableSchema,
                         column_prop: Union[TColumnProp, str],
                         include_incomplete: bool = False) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L678)

Checks if `table` schema contains column with property `column_prop`.

## get\_dedup\_sort\_tuple

```python
def get_dedup_sort_tuple(
        table: TTableSchema,
        include_incomplete: bool = False) -> Optional[Tuple[str, TSortOrder]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L685)

Returns tuple with dedup sort information.

First element is the sort column name, second element is the sort order.

Returns None if "dedup_sort" hint was not provided.

## get\_write\_disposition

```python
def get_write_disposition(tables: TSchemaTables,
                          table_name: str) -> TWriteDisposition
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L745)

Returns table hint of a table if present. If not, looks up into parent table

## fill\_hints\_from\_parent\_and\_clone\_table

```python
def fill_hints_from_parent_and_clone_table(
        tables: TSchemaTables, table: TTableSchema) -> TTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L772)

Takes write disposition and table format from parent tables if not present

## table\_schema\_has\_type

```python
def table_schema_has_type(table: TTableSchema, _typ: TDataType) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L793)

Checks if `table` schema contains column with type _typ

## table\_schema\_has\_type\_with\_precision

```python
def table_schema_has_type_with_precision(table: TTableSchema,
                                         _typ: TDataType) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L798)

Checks if `table` schema contains column with type _typ and precision set

## get\_root\_table

```python
def get_root_table(tables: TSchemaTables, table_name: str) -> TTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L806)

Finds root (without parent) of a `table_name` following the nested references (row_key - parent_key).

## get\_nested\_tables

```python
def get_nested_tables(tables: TSchemaTables,
                      table_name: str) -> List[TTableSchema]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L814)

Get nested tables for table name and return a list of tables ordered by ancestry so the nested tables are always after their parents

Note that this function follows only NESTED TABLE reference typically expressed on _dlt_parent_id (PARENT_KEY) to _dlt_id (ROW_KEY).

## group\_tables\_by\_resource

```python
def group_tables_by_resource(
        tables: TSchemaTables,
        pattern: Optional[REPattern] = None) -> Dict[str, List[TTableSchema]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L832)

Create a dict of resources and their associated tables and descendant tables
If `pattern` is supplied, the result is filtered to only resource names matching the pattern.

## dlt\_id\_column

```python
def dlt_id_column() -> TColumnSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L913)

Definition of dlt id column

## dlt\_load\_id\_column

```python
def dlt_load_id_column() -> TColumnSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/utils.py#L924)

Definition of dlt load id column

