---
sidebar_label: hints
title: extract.hints
---

## make\_hints

```python
def make_hints(
        table_name: TTableHintTemplate[str] = None,
        parent_table_name: TTableHintTemplate[str] = None,
        write_disposition: TTableHintTemplate[TWriteDispositionConfig] = None,
        columns: TTableHintTemplate[TAnySchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnNames] = None,
        merge_key: TTableHintTemplate[TColumnNames] = None,
        schema_contract: TTableHintTemplate[TSchemaContract] = None,
        table_format: TTableHintTemplate[TTableFormat] = None,
        file_format: TTableHintTemplate[TFileFormat] = None,
        references: TTableHintTemplate[TTableReferenceParam] = None,
        incremental: TIncrementalConfig = None) -> TResourceHints
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/hints.py#L77)

A convenience function to create resource hints. Accepts both static and dynamic hints based on data.

This method accepts the same table hints arguments as `dlt.resource` decorator.

## DltResourceHints Objects

```python
class DltResourceHints()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/hints.py#L127)

### table\_name

```python
@property
def table_name() -> TTableHintTemplate[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/hints.py#L144)

Get table name to which resource loads data. May return a callable.

### columns

```python
@property
def columns() -> TTableHintTemplate[TTableSchemaColumns]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/hints.py#L166)

Gets columns' schema that can be modified in place

### compute\_table\_schema

```python
def compute_table_schema(item: TDataItem = None,
                         meta: Any = None) -> TTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/hints.py#L182)

Computes the table schema based on hints and column definitions passed during resource creation.
`item` parameter is used to resolve table hints based on data.
`meta` parameter is taken from Pipe and may further specify table name if variant is to be used

### apply\_hints

```python
def apply_hints(
        table_name: TTableHintTemplate[str] = None,
        parent_table_name: TTableHintTemplate[str] = None,
        write_disposition: TTableHintTemplate[TWriteDispositionConfig] = None,
        columns: TTableHintTemplate[TAnySchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnNames] = None,
        merge_key: TTableHintTemplate[TColumnNames] = None,
        incremental: TIncrementalConfig = None,
        schema_contract: TTableHintTemplate[TSchemaContract] = None,
        additional_table_hints: Optional[Dict[str,
                                              TTableHintTemplate[Any]]] = None,
        table_format: TTableHintTemplate[TTableFormat] = None,
        file_format: TTableHintTemplate[TFileFormat] = None,
        references: TTableHintTemplate[TTableReferenceParam] = None,
        create_table_variant: bool = False) -> Self
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/hints.py#L222)

Creates or modifies existing table schema by setting provided hints. Accepts both static and dynamic hints based on data.

If `create_table_variant` is specified, the `table_name` must be a string and hints will be used to create a separate set of hints
for a particular `table_name`. Such hints may be retrieved via compute_table_schema(meta=TableNameMeta(table_name)).
Table variant hints may not contain dynamic hints.

This method accepts the same table hints arguments as `dlt.resource` decorator with the following additions.
Skip the argument or pass None to leave the existing hint.
Pass empty value (for a particular type i.e. "" for a string) to remove a hint.

parent_table_name (str, optional): A name of parent table if foreign relation is defined. Please note that if you use merge, you must define `root_key` columns explicitly
incremental (Incremental, optional): Enables the incremental loading for a resource.

Please note that for efficient incremental loading, the resource must be aware of the Incremental by accepting it as one if its arguments and then using are to skip already loaded data.
In non-aware resources, `dlt` will filter out the loaded values, however, the resource will yield all the values again.

Returns: self for chaining

