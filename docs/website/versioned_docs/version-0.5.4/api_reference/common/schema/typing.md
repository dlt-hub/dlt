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

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/schema/typing.py#L99)

TypedDict that defines basic properties of a column: name, data type and nullable

## TColumnSchema Objects

```python
class TColumnSchema(TColumnSchemaBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/schema/typing.py#L106)

TypedDict that defines additional column hints

## TTableSchemaColumns

A mapping from column name to column schema, typically part of a table schema

## TSchemaContractDict Objects

```python
class TSchemaContractDict(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/schema/typing.py#L139)

TypedDict defining the schema update settings

## DEFAULT\_VALIDITY\_COLUMN\_NAMES

Default values for validity column names used in `scd2` merge strategy.

## TTableSchema Objects

```python
class TTableSchema(TTableProcessingHints)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/schema/typing.py#L198)

TypedDict that defines properties of a table

## TStoredSchema Objects

```python
class TStoredSchema(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/schema/typing.py#L229)

TypeDict defining the schema representation in storage

