---
sidebar_label: debug_pipeline
title: sources._single_file_templates.debug_pipeline
---

The Debug Pipeline Template will load a column with each datatype to your destination.

## resource

```python
@dlt.resource(write_disposition="append", name="all_datatypes")
def resource()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/debug_pipeline.py#L11)

this is the test data for loading validation, delete it once you yield actual data

## source

```python
@dlt.source
def source()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/debug_pipeline.py#L37)

A source function groups all resources into one schema.

