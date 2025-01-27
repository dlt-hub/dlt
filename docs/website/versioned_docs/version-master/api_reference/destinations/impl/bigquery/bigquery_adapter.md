---
sidebar_label: bigquery_adapter
title: destinations.impl.bigquery.bigquery_adapter
---

## bigquery\_adapter

```python
def bigquery_adapter(
        data: Any,
        partition: TColumnNames = None,
        cluster: TColumnNames = None,
        round_half_away_from_zero: TColumnNames = None,
        round_half_even: TColumnNames = None,
        table_description: Optional[str] = None,
        table_expiration_datetime: Optional[str] = None,
        insert_api: Optional[Literal["streaming", "default"]] = None,
        autodetect_schema: Optional[bool] = None,
        partition_expiration_days: Optional[int] = None) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/bigquery/bigquery_adapter.py#L28)

Prepares data for loading into BigQuery.

This function takes data, which can be raw or already wrapped in a DltResource object,
and prepares it for BigQuery by optionally specifying partitioning, clustering, table description and
table expiration settings.

**Arguments**:

- `data` _Any_ - The data to be transformed.
  This can be raw data or an instance of DltResource.
  If raw data is provided, the function will wrap it into a `DltResource` object.
- `partition` _TColumnNames, optional_ - The name of the column to partition the BigQuery table by.
  This should be a string representing a single column name.
- `cluster` _TColumnNames, optional_ - A column name or list of column names to cluster the BigQuery table by.
- `round_half_away_from_zero` _TColumnNames, optional_ - Determines how values in the column are rounded when written to the table.
  This mode rounds halfway cases away from zero.
  The columns specified must be mutually exclusive from `round_half_even`.
  See https://cloud.google.com/bigquery/docs/schemas#rounding_mode for more information.
- `round_half_even` _TColumnNames, optional_ - Determines how values in the column are rounded when written to the table.
  This mode rounds halfway cases towards the nearest even digit.
  The columns specified must be mutually exclusive from `round_half_away_from_zero`.
  See https://cloud.google.com/bigquery/docs/schemas#rounding_mode for more information.
- `table_description` _str, optional_ - A description for the BigQuery table.
- `table_expiration_datetime` _str, optional_ - String representing the datetime when the BigQuery table expires.
  This is always interpreted as UTC, BigQuery's default.
- `insert_api` _Optional[Literal["streaming", "default"]]_ - The API to use for inserting data into BigQuery.
  If "default" is chosen, the original SQL query mechanism is used.
  If "streaming" is chosen, the streaming API (https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery)
  is used.
- `NOTE` - due to BigQuery features, streaming insert is only available for `append` write_disposition.
- `autodetect_schema` _bool, optional_ - If set to True, BigQuery schema autodetection will be used to create data tables. This
  allows to create structured types from nested data.
- `partition_expiration_days` _int, optional_ - For date/time based partitions it tells when partition is expired and removed.
  Partitions are expired based on a partitioned column value. (https://cloud.google.com/bigquery/docs/managing-partitioned-tables#partition-expiration)
  

**Returns**:

  A `DltResource` object that is ready to be loaded into BigQuery.
  

**Raises**:

- `ValueError` - If any hint is invalid or none are specified.
  

**Examples**:

```py
    data = [{"name": "Marcel", "description": "Raccoon Engineer", "date_hired": 1700784000}]
    bigquery_adapter(data, partition="date_hired", table_expiration_datetime="2024-01-30", table_description="Employee Data")
```
  [DltResource with hints applied]

## should\_autodetect\_schema

```python
def should_autodetect_schema(table: PreparedTableSchema) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/bigquery/bigquery_adapter.py#L190)

Tells if schema should be auto detected for a given prepared `table`

