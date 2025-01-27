---
sidebar_label: default_pipeline
title: sources._single_file_templates.default_pipeline
---

The Intro Pipeline Template contains the example from the docs intro page

## load\_api\_data

```python
def load_api_data() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/default_pipeline.py#L13)

Load data from the chess api, for more complex examples use our rest_api source

## load\_pandas\_data

```python
def load_pandas_data() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/default_pipeline.py#L33)

Load data from a public csv via pandas

## load\_sql\_data

```python
def load_sql_data() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/default_pipeline.py#L53)

Load data from a sql database with sqlalchemy, for more complex examples use our sql_database source

