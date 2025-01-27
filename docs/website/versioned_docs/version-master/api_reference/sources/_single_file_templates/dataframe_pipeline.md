---
sidebar_label: dataframe_pipeline
title: sources._single_file_templates.dataframe_pipeline
---

The DataFrame Pipeline Template will show how to load and transform pandas dataframes.

## resource

```python
@dlt.resource(write_disposition="append", name="people")
def resource()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/dataframe_pipeline.py#L15)

One resource function will materialize as a table in the destination, wie yield example data here

## add\_updated\_at

```python
def add_updated_at(item: pd.DataFrame)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/dataframe_pipeline.py#L20)

Map function to add an updated at column to your incoming data.

## source

```python
@dlt.source
def source()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/dataframe_pipeline.py#L33)

A source function groups all resources into one schema.

