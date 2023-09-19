---
sidebar_label: typing
title: common.schema.typing
---

#### TColumnProp

Known properties and hints of the column

#### TColumnHint

Known hints of a column used to declare hint regexes.

#### TColumnNames

A string representing a column name or a list of

## TColumnSchemaBase Objects

```python
class TColumnSchemaBase(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/schema/typing.py#L37)

TypedDict that defines basic properties of a column: name, data type and nullable

## TColumnSchema Objects

```python
class TColumnSchema(TColumnSchemaBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/schema/typing.py#L44)

TypedDict that defines additional column hints

#### TTableSchemaColumns

A mapping from column name to column schema, typically part of a table schema

## TTableSchema Objects

```python
class TTableSchema(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/schema/typing.py#L74)

TypedDict that defines properties of a table

## TStoredSchema Objects

```python
class TStoredSchema(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/schema/typing.py#L100)

TypeDict defining the schema representation in storage

