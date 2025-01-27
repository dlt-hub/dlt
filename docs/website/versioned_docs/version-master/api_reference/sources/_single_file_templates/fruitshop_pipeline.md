---
sidebar_label: fruitshop_pipeline
title: sources._single_file_templates.fruitshop_pipeline
---

The Default Pipeline Template provides a simple starting point for your dlt pipeline

## customers

```python
@dlt.resource(primary_key="id")
def customers()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/fruitshop_pipeline.py#L10)

Load customer data from a simple python list.

## inventory

```python
@dlt.resource(primary_key="id")
def inventory()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/fruitshop_pipeline.py#L20)

Load inventory data from a simple python list.

## fruitshop

```python
@dlt.source
def fruitshop()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/fruitshop_pipeline.py#L30)

A source function groups all resources into one schema.

