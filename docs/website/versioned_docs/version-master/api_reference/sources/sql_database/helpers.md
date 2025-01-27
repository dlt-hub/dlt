---
sidebar_label: helpers
title: sources.sql_database.helpers
---

SQL database source helpers

## unwrap\_json\_connector\_x

```python
def unwrap_json_connector_x(field: str) -> TDataItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/sql_database/helpers.py#L319)

Creates a transform function to be added with `add_map` that will unwrap JSON columns
ingested via connectorx. Such columns are additionally quoted and translate SQL NULL to json "null"

## remove\_nullability\_adapter

```python
def remove_nullability_adapter(table: Table) -> Table
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/sql_database/helpers.py#L341)

A table adapter that removes nullability from columns.

## SqlTableResourceConfiguration Objects

```python
@configspec
class SqlTableResourceConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/sql_database/helpers.py#L394)

### incremental

type: ignore[type-arg]

