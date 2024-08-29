---
sidebar_label: escape
title: common.data_writers.escape
---

## format\_datetime\_literal

```python
def format_datetime_literal(v: pendulum.DateTime,
                            precision: int = 6,
                            no_tz: bool = False) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/data_writers/escape.py#L196)

Converts `v` to ISO string, optionally without timezone spec (in UTC) and with given `precision`

## format\_bigquery\_datetime\_literal

```python
def format_bigquery_datetime_literal(v: pendulum.DateTime,
                                     precision: int = 6,
                                     no_tz: bool = False) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/data_writers/escape.py#L210)

Returns BigQuery-adjusted datetime literal by prefixing required `TIMESTAMP` indicator.

Also works for Presto-based engines.

## format\_clickhouse\_datetime\_literal

```python
def format_clickhouse_datetime_literal(v: pendulum.DateTime,
                                       precision: int = 6,
                                       no_tz: bool = False) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/data_writers/escape.py#L221)

Returns clickhouse compatibel function

