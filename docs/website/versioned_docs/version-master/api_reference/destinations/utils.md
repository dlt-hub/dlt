---
sidebar_label: utils
title: destinations.utils
---

## get\_resource\_for\_adapter

```python
def get_resource_for_adapter(data: Any) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/utils.py#L26)

Helper function for adapters. Wraps `data` in a DltResource if it's not a DltResource already.
Alternatively if `data` is a DltSource, throws an error if there are multiple resource in the source
or returns the single resource if available.

## info\_schema\_null\_to\_bool

```python
def info_schema_null_to_bool(v: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/utils.py#L53)

Converts INFORMATION SCHEMA truth values to Python bool

## parse\_db\_data\_type\_str\_with\_precision

```python
def parse_db_data_type_str_with_precision(
        db_type: str) -> Tuple[str, Optional[int], Optional[int]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/utils.py#L62)

Parses a db data type with optional precision or precision and scale information

## get\_pipeline\_state\_query\_columns

```python
def get_pipeline_state_query_columns() -> TTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/utils.py#L78)

We get definition of pipeline state table without columns we do not need for the query

