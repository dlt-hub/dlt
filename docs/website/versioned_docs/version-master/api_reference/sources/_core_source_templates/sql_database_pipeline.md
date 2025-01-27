---
sidebar_label: sql_database_pipeline
title: sources._core_source_templates.sql_database_pipeline
---

## load\_select\_tables\_from\_database

```python
def load_select_tables_from_database() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/sql_database_pipeline.py#L16)

Use the sql_database source to reflect an entire database schema and load select tables from it.

This example sources data from the public Rfam MySQL database.

## load\_entire\_database

```python
def load_entire_database() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/sql_database_pipeline.py#L57)

Use the sql_database source to completely load all tables in a database

## load\_standalone\_table\_resource

```python
def load_standalone_table_resource() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/sql_database_pipeline.py#L71)

Load a few known tables with the standalone sql_table resource, request full schema and deferred
table reflection

## select\_columns

```python
def select_columns() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/sql_database_pipeline.py#L115)

Uses table adapter callback to modify list of columns to be selected

## select\_with\_end\_value\_and\_row\_order

```python
def select_with_end_value_and_row_order() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/sql_database_pipeline.py#L148)

Gets data from a table withing a specified range and sorts rows descending

## my\_sql\_via\_pyarrow

```python
def my_sql_via_pyarrow() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/sql_database_pipeline.py#L175)

Uses pyarrow backend to load tables from mysql

## create\_unsw\_flow

```python
def create_unsw_flow() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/sql_database_pipeline.py#L205)

Uploads UNSW_Flow dataset to postgres via csv stream skipping dlt normalizer.
You need to download the dataset from https://github.com/rdpahalavan/nids-datasets

## test\_connectorx\_speed

```python
def test_connectorx_speed() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/sql_database_pipeline.py#L239)

Uses unsw_flow dataset (~2mln rows, 25+ columns) to test connectorx speed

## use\_type\_adapter

```python
def use_type_adapter() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/sql_database_pipeline.py#L304)

Example use of type adapter to coerce unknown data types

## specify\_columns\_to\_load

```python
def specify_columns_to_load() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/sql_database_pipeline.py#L328)

Run the SQL database source with a subset of table columns loaded

