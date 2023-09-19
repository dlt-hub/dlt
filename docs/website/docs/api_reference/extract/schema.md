---
sidebar_label: schema
title: extract.schema
---

## DltResourceSchema Objects

```python
class DltResourceSchema()
```

#### table\_name

```python
@property
def table_name() -> TTableHintTemplate[str]
```

Get table name to which resource loads data. May return a callable.

#### columns

```python
@property
def columns() -> TTableHintTemplate[TTableSchemaColumns]
```

Gets columns schema that can be modified in place

#### compute\_table\_schema

```python
def compute_table_schema(item: TDataItem = None) -> TPartialTableSchema
```

Computes the table schema based on hints and column definitions passed during resource creation. `item` parameter is used to resolve table hints based on data

#### apply\_hints

```python
def apply_hints(
        table_name: TTableHintTemplate[str] = None,
        parent_table_name: TTableHintTemplate[str] = None,
        write_disposition: TTableHintTemplate[TWriteDisposition] = None,
        columns: TTableHintTemplate[TAnySchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnNames] = None,
        merge_key: TTableHintTemplate[TColumnNames] = None,
        incremental: Incremental[Any] = None) -> None
```

Creates or modifies existing table schema by setting provided hints. Accepts both static and dynamic hints based on data.

This method accepts the same table hints arguments as `dlt.resource` decorator with the following additions.
Skip the argument or pass None to leave the existing hint.
Pass empty value (for particular type ie "" for a string) to remove hint

parent_table_name (str, optional): A name of parent table if foreign relation is defined. Please note that if you use merge you must define `root_key` columns explicitly
incremental (Incremental, optional): Enables the incremental loading for a resource.

Please note that for efficient incremental loading, the resource must be aware of the Incremental by accepting it as one if its arguments and then using is to skip already loaded data.
In non-aware resources, `dlt` will filter out the loaded values, however the resource will yield all the values again.

