---
sidebar_label: arrow_pipeline
title: sources._single_file_templates.arrow_pipeline
---

The Arrow Pipeline Template will show how to load and transform arrow tables.

## resource

```python
@dlt.resource(write_disposition="append", name="people")
def resource()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/arrow_pipeline.py#L15)

One resource function will materialize as a table in the destination, wie yield example data here

## add\_updated\_at

```python
def add_updated_at(item: pa.Table)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/arrow_pipeline.py#L20)

Map function to add an updated at column to your incoming data.

## source

```python
@dlt.source
def source()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/arrow_pipeline.py#L32)

A source function groups all resources into one schema.

