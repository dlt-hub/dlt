---
sidebar_label: typing
title: common.schema.typing
---

## C\_DLT\_ID

unique id of current row

## C\_DLT\_LOAD\_ID

load id to identify records loaded in a single load package

## TColumnProp

All known properties of the column, including name, data type info and hints

## TColumnHint

Known hints of a column

## TColumnSchemaBase Objects

```python
class TColumnSchemaBase(TColumnType)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/typing.py#L145)

TypedDict that defines basic properties of a column: name, data type and nullable

## TColumnSchema Objects

```python
class TColumnSchema(TColumnSchemaBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/typing.py#L151)

TypedDict that defines additional column hints

## TTableSchemaColumns

A mapping from column name to column schema, typically part of a table schema

## TSchemaContractDict Objects

```python
class TSchemaContractDict(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/typing.py#L186)

TypedDict defining the schema update settings

## DEFAULT\_VALIDITY\_COLUMN\_NAMES

Default values for validity column names used in `scd2` merge strategy.

## TTableReference Objects

```python
class TTableReference(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/typing.py#L250)

Describes a reference to another table's columns.
`columns` corresponds to the `referenced_columns` in the referenced table and their order should match.

## TTableSchema Objects

```python
class TTableSchema(_TTableSchemaBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/typing.py#L276)

TypedDict that defines properties of a table

## TColumnDefaultHint

Allows using not_null in default hints setting section

## TStoredSchema Objects

```python
class TStoredSchema(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/typing.py#L300)

TypeDict defining the schema representation in storage

