---
sidebar_label: athena_adapter
title: destinations.impl.athena.athena_adapter
---

## PartitionTransformation Objects

```python
class PartitionTransformation()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/athena/athena_adapter.py#L15)

### template

Template string of the transformation including column name placeholder. E.g. `bucket(16, {column_name})`

### column\_name

Column name to apply the transformation to

## athena\_partition Objects

```python
class athena_partition()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/athena/athena_adapter.py#L26)

Helper class to generate iceberg partition transformations

E.g. `athena_partition.bucket(16, "id")` will return a transformation with template `bucket(16, {column_name})`
This can be correctly rendered by the athena loader with escaped column name.

### year

```python
@staticmethod
def year(column_name: str) -> PartitionTransformation
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/athena/athena_adapter.py#L34)

Partition by year part of a date or timestamp column.

### month

```python
@staticmethod
def month(column_name: str) -> PartitionTransformation
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/athena/athena_adapter.py#L39)

Partition by month part of a date or timestamp column.

### day

```python
@staticmethod
def day(column_name: str) -> PartitionTransformation
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/athena/athena_adapter.py#L44)

Partition by day part of a date or timestamp column.

### hour

```python
@staticmethod
def hour(column_name: str) -> PartitionTransformation
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/athena/athena_adapter.py#L49)

Partition by hour part of a date or timestamp column.

### bucket

```python
@staticmethod
def bucket(n: int, column_name: str) -> PartitionTransformation
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/athena/athena_adapter.py#L54)

Partition by hashed value to n buckets.

### truncate

```python
@staticmethod
def truncate(length: int, column_name: str) -> PartitionTransformation
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/athena/athena_adapter.py#L59)

Partition by value truncated to length.

## athena\_adapter

```python
def athena_adapter(
    data: Any,
    partition: Union[str, PartitionTransformation,
                     Sequence[Union[str, PartitionTransformation]]] = None
) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/athena/athena_adapter.py#L64)

Prepares data for loading into Athena

**Arguments**:

- `data` - The data to be transformed.
  This can be raw data or an instance of DltResource.
  If raw data is provided, the function will wrap it into a `DltResource` object.
- `partition` - Column name(s) or instances of `PartitionTransformation` to partition the table by.
  To use a transformation it's best to use the methods of the helper class `athena_partition`
  to generate correctly escaped SQL in the loader.
  

**Returns**:

  A `DltResource` object that is ready to be loaded into BigQuery.
  

**Raises**:

- `ValueError` - If any hint is invalid or none are specified.
  

**Examples**:

```py
    data = [{"name": "Marcel", "department": "Engineering", "date_hired": "2024-01-30"}]
    athena_adapter(data, partition=["department", athena_partition.year("date_hired"), athena_partition.bucket(8, "name")])
```
  [DltResource with hints applied]

