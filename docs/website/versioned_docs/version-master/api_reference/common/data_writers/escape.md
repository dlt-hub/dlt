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

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/data_writers/escape.py#L213)

Converts `v` to ISO string, optionally without timezone spec (in UTC) and with given `precision`

## format\_bigquery\_datetime\_literal

```python
def format_bigquery_datetime_literal(v: pendulum.DateTime,
                                     precision: int = 6,
                                     no_tz: bool = False) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/data_writers/escape.py#L227)

Returns BigQuery-adjusted datetime literal by prefixing required `TIMESTAMP` indicator.

Also works for Presto-based engines.

## format\_clickhouse\_datetime\_literal

```python
def format_clickhouse_datetime_literal(v: pendulum.DateTime,
                                       precision: int = 6,
                                       no_tz: bool = False) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/data_writers/escape.py#L238)

Returns clickhouse compatibel function

