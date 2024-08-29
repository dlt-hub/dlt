---
sidebar_label: typing
title: common.schema.typing
---

## TColumnProp

Known properties and hints of the column

## TColumnHint

Known hints of a column used to declare hint regexes.

## TColumnNames

A string representing a column name or a list of

## TColumnSchemaBase Objects

```python
class TColumnSchemaBase(TColumnType)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/typing.py#L98)

TypedDict that defines basic properties of a column: name, data type and nullable

## TColumnSchema Objects

```python
class TColumnSchema(TColumnSchemaBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/typing.py#L105)

TypedDict that defines additional column hints

## TTableSchemaColumns

A mapping from column name to column schema, typically part of a table schema

## TSchemaContractDict Objects

```python
class TSchemaContractDict(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/typing.py#L138)

TypedDict defining the schema update settings

## TTableSchema Objects

```python
class TTableSchema(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/typing.py#L161)

TypedDict that defines properties of a table

## TStoredSchema Objects

```python
class TStoredSchema(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/typing.py#L191)

TypeDict defining the schema representation in storage

