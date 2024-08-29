---
sidebar_label: hints
title: extract.hints
---

## make\_hints

```python
def make_hints(
        table_name: TTableHintTemplate[str] = None,
        parent_table_name: TTableHintTemplate[str] = None,
        write_disposition: TTableHintTemplate[TWriteDisposition] = None,
        columns: TTableHintTemplate[TAnySchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnNames] = None,
        merge_key: TTableHintTemplate[TColumnNames] = None,
        schema_contract: TTableHintTemplate[TSchemaContract] = None,
        table_format: TTableHintTemplate[TTableFormat] = None
) -> TResourceHints
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/hints.py#L54)

A convenience function to create resource hints. Accepts both static and dynamic hints based on data.

This method accepts the same table hints arguments as `dlt.resource` decorator.

## DltResourceHints Objects

```python
class DltResourceHints()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/hints.py#L102)

### table\_name

```python
@property
def table_name() -> TTableHintTemplate[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/hints.py#L116)

Get table name to which resource loads data. May return a callable.

### columns

```python
@property
def columns() -> TTableHintTemplate[TTableSchemaColumns]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/hints.py#L138)

Gets columns' schema that can be modified in place

### compute\_table\_schema

```python
def compute_table_schema(item: TDataItem = None) -> TTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/hints.py#L146)

Computes the table schema based on hints and column definitions passed during resource creation. `item` parameter is used to resolve table hints based on data.

### apply\_hints

```python
def apply_hints(
        table_name: TTableHintTemplate[str] = None,
        parent_table_name: TTableHintTemplate[str] = None,
        write_disposition: TTableHintTemplate[TWriteDisposition] = None,
        columns: TTableHintTemplate[TAnySchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnNames] = None,
        merge_key: TTableHintTemplate[TColumnNames] = None,
        incremental: Incremental[Any] = None,
        schema_contract: TTableHintTemplate[TSchemaContract] = None,
        additional_table_hints: Optional[Dict[str,
                                              TTableHintTemplate[Any]]] = None,
        table_format: TTableHintTemplate[TTableFormat] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/hints.py#L175)

Creates or modifies existing table schema by setting provided hints. Accepts both static and dynamic hints based on data.

This method accepts the same table hints arguments as `dlt.resource` decorator with the following additions.
Skip the argument or pass None to leave the existing hint.
Pass empty value (for a particular type i.e. "" for a string) to remove a hint.

parent_table_name (str, optional): A name of parent table if foreign relation is defined. Please note that if you use merge, you must define `root_key` columns explicitly
incremental (Incremental, optional): Enables the incremental loading for a resource.

Please note that for efficient incremental loading, the resource must be aware of the Incremental by accepting it as one if its arguments and then using are to skip already loaded data.
In non-aware resources, `dlt` will filter out the loaded values, however, the resource will yield all the values again.

