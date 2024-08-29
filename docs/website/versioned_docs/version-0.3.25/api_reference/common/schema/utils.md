---
sidebar_label: utils
title: common.schema.utils
---

## is\_valid\_schema\_name

```python
def is_valid_schema_name(name: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L52)

Schema name must be a valid python identifier and have max len of 64

## normalize\_schema\_name

```python
def normalize_schema_name(name: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L61)

Normalizes schema name by using snake case naming convention. The maximum length is 64 characters

## apply\_defaults

```python
def apply_defaults(stored_schema: TStoredSchema) -> TStoredSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L67)

Applies default hint values to `stored_schema` in place

Updates only complete column hints, incomplete columns are preserved intact

## remove\_defaults

```python
def remove_defaults(stored_schema: TStoredSchema) -> TStoredSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L91)

Removes default values from `stored_schema` in place, returns the input for chaining

Default values are removed from table schemas and complete column schemas. Incomplete columns are preserved intact.

## has\_default\_column\_hint\_value

```python
def has_default_column_hint_value(hint: str, value: Any) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L114)

Checks if `value` is a default for `hint`. Only known column hints (COLUMN_HINTS) are checked

## remove\_column\_defaults

```python
def remove_column_defaults(column_schema: TColumnSchema) -> TColumnSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L124)

Removes default values from `column_schema` in place, returns the input for chaining

## add\_column\_defaults

```python
def add_column_defaults(column: TColumnSchemaBase) -> TColumnSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L135)

Adds default boolean hints to column

## bump\_version\_if\_modified

```python
def bump_version_if_modified(
        stored_schema: TStoredSchema) -> Tuple[int, str, str, Sequence[str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L163)

Bumps the `stored_schema` version and version hash if content modified, returns (new version, new hash, old hash, 10 last hashes) tuple

## compile\_simple\_regexes

```python
def compile_simple_regexes(r: Iterable[TSimpleRegex]) -> REPattern
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L299)

Compile multiple patterns as one

## is\_complete\_column

```python
def is_complete_column(col: TColumnSchemaBase) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L331)

Returns true if column contains enough data to be created at the destination. Must contain a name and a data type. Other hints have defaults.

## compare\_complete\_columns

```python
def compare_complete_columns(a: TColumnSchema, b: TColumnSchema) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L336)

Compares mandatory fields of complete columns

## merge\_columns

```python
def merge_columns(col_a: TColumnSchema,
                  col_b: TColumnSchema,
                  merge_defaults: bool = True) -> TColumnSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L343)

Merges `col_b` into `col_a`. if `merge_defaults` is True, only hints from `col_b` that are not default in `col_a` will be set.

Modifies col_a in place and returns it

## diff\_tables

```python
def diff_tables(tab_a: TTableSchema,
                tab_b: TPartialTableSchema) -> TPartialTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L357)

Creates a partial table that contains properties found in `tab_b` that are not present or different in `tab_a`.
The name is always present in returned partial.
It returns new columns (not present in tab_a) and merges columns from tab_b into tab_a (overriding non-default hint values).
If any columns are returned they contain full data (not diffs of columns)

Raises SchemaException if tables cannot be merged
* when columns with the same name  have different data types
* when table links to different parent tables

## merge\_tables

```python
def merge_tables(table: TTableSchema,
                 partial_table: TPartialTableSchema) -> TPartialTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L431)

Merges "partial_table" into "table". `table` is merged in place. Returns the diff partial table.

`table` and `partial_table` names must be identical. A table diff is generated and applied to `table`:
* new columns are added, updated columns are replaced from diff
* table hints are added or replaced from diff
* nothing gets deleted

## has\_table\_seen\_data

```python
def has_table_seen_data(table: TTableSchema) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L454)

Checks if normalizer has seen data coming to the table.

## get\_first\_column\_name\_with\_prop

```python
def get_first_column_name_with_prop(
        table: TTableSchema,
        column_prop: Union[TColumnProp, str],
        include_incomplete: bool = False) -> Optional[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L477)

Returns name of first column in `table` schema with property `column_prop` or None if no such column exists.

## has\_column\_with\_prop

```python
def has_column_with_prop(table: TTableSchema,
                         column_prop: Union[TColumnProp, str],
                         include_incomplete: bool = False) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L487)

Checks if `table` schema contains column with property `column_prop`.

## get\_dedup\_sort\_tuple

```python
def get_dedup_sort_tuple(
        table: TTableSchema,
        include_incomplete: bool = False) -> Optional[Tuple[str, TSortOrder]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L494)

Returns tuple with dedup sort information.

First element is the sort column name, second element is the sort order.

Returns None if "dedup_sort" hint was not provided.

## get\_write\_disposition

```python
def get_write_disposition(tables: TSchemaTables,
                          table_name: str) -> TWriteDisposition
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L541)

Returns table hint of a table if present. If not, looks up into parent table

## fill\_hints\_from\_parent\_and\_clone\_table

```python
def fill_hints_from_parent_and_clone_table(
        tables: TSchemaTables, table: TTableSchema) -> TTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L555)

Takes write disposition and table format from parent tables if not present

## table\_schema\_has\_type

```python
def table_schema_has_type(table: TTableSchema, _typ: TDataType) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L569)

Checks if `table` schema contains column with type _typ

## table\_schema\_has\_type\_with\_precision

```python
def table_schema_has_type_with_precision(table: TTableSchema,
                                         _typ: TDataType) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L574)

Checks if `table` schema contains column with type _typ and precision set

## get\_top\_level\_table

```python
def get_top_level_table(tables: TSchemaTables,
                        table_name: str) -> TTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L582)

Finds top level (without parent) of a `table_name` following the ancestry hierarchy.

## get\_child\_tables

```python
def get_child_tables(tables: TSchemaTables,
                     table_name: str) -> List[TTableSchema]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L591)

Get child tables for table name and return a list of tables ordered by ancestry so the child tables are always after their parents

## group\_tables\_by\_resource

```python
def group_tables_by_resource(
        tables: TSchemaTables,
        pattern: Optional[REPattern] = None) -> Dict[str, List[TTableSchema]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/utils.py#L606)

Create a dict of resources and their associated tables and descendant tables
If `pattern` is supplied, the result is filtered to only resource names matching the pattern.

