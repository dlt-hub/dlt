---
sidebar_label: utils
title: destinations.utils
---

## get\_resource\_for\_adapter

```python
def get_resource_for_adapter(data: Any) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/utils.py#L24)

Helper function for adapters. Wraps `data` in a DltResource if it's not a DltResource already.
Alternatively if `data` is a DltSource, throws an error if there are multiple resource in the source
or returns the single resource if available.

## info\_schema\_null\_to\_bool

```python
def info_schema_null_to_bool(v: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/utils.py#L51)

Converts INFORMATION SCHEMA truth values to Python bool

## parse\_db\_data\_type\_str\_with\_precision

```python
def parse_db_data_type_str_with_precision(
        db_type: str) -> Tuple[str, Optional[int], Optional[int]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/utils.py#L60)

Parses a db data type with optional precision or precision and scale information

## get\_pipeline\_state\_query\_columns

```python
def get_pipeline_state_query_columns() -> TTableSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/utils.py#L76)

We get definition of pipeline state table without columns we do not need for the query

