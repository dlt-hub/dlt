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

TypedDict that defines basic properties of a column: name, data type and nullable

## TColumnSchema Objects

```python
class TColumnSchema(TColumnSchemaBase)
```

TypedDict that defines additional column hints

#### TTableSchemaColumns

A mapping from column name to column schema, typically part of a table schema

## TTableSchema Objects

```python
class TTableSchema(TypedDict)
```

TypedDict that defines properties of a table

## TStoredSchema Objects

```python
class TStoredSchema(TypedDict)
```

TypeDict defining the schema representation in storage

