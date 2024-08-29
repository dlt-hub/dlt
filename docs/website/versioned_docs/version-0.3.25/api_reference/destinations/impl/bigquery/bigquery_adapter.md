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
        table_expiration_datetime: Optional[str] = None) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/bigquery/bigquery_adapter.py#L25)

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

