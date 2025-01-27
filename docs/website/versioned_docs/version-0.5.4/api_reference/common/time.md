---
sidebar_label: time
title: common.time
---

## precise\_time

A precise timer using win_precise_time library on windows and time.time on other systems

## timestamp\_within

```python
def timestamp_within(timestamp: float, min_exclusive: Optional[float],
                     max_inclusive: Optional[float]) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/time.py#L32)

check if timestamp within range uniformly treating none and range inclusiveness

## timestamp\_before

```python
def timestamp_before(timestamp: float, max_inclusive: Optional[float]) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/time.py#L43)

check if timestamp is before max timestamp, inclusive

## parse\_iso\_like\_datetime

```python
def parse_iso_like_datetime(
        value: Any) -> Union[pendulum.DateTime, pendulum.Date, pendulum.Time]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/time.py#L50)

Parses ISO8601 string into pendulum datetime, date or time. Preserves timezone info.
Note: naive datetimes will be generated from string without timezone

we use internal pendulum parse function. the generic function, for example, parses string "now" as now()
it also tries to parse ISO intervals but the code is very low quality

## ensure\_pendulum\_date

```python
def ensure_pendulum_date(value: TAnyDateTime) -> pendulum.Date
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/time.py#L73)

Coerce a date/time value to a `pendulum.Date` object.

UTC is assumed if the value is not timezone aware. Other timezones are shifted to UTC

**Arguments**:

- `value` - The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.
  

**Returns**:

  A timezone aware pendulum.Date object.

## ensure\_pendulum\_datetime

```python
def ensure_pendulum_datetime(value: TAnyDateTime) -> pendulum.DateTime
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/time.py#L100)

Coerce a date/time value to a `pendulum.DateTime` object.

UTC is assumed if the value is not timezone aware. Other timezones are shifted to UTC

**Arguments**:

- `value` - The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.
  

**Returns**:

  A timezone aware pendulum.DateTime object in UTC timezone.

## ensure\_pendulum\_time

```python
def ensure_pendulum_time(value: Union[str, datetime.time]) -> pendulum.Time
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/time.py#L127)

Coerce a time value to a `pendulum.Time` object.

**Arguments**:

- `value` - The value to coerce. Can be a `pendulum.Time` / `datetime.time` or an iso time string.
  

**Returns**:

  A pendulum.Time object

## to\_py\_datetime

```python
def to_py_datetime(value: datetime.datetime) -> datetime.datetime
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/time.py#L149)

Convert a pendulum.DateTime to a py datetime object.

**Arguments**:

- `value` - The value to convert. Can be a pendulum.DateTime or datetime.
  

**Returns**:

  A py datetime object

## to\_py\_date

```python
def to_py_date(value: datetime.date) -> datetime.date
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/time.py#L172)

Convert a pendulum.Date to a py date object.

**Arguments**:

- `value` - The value to convert. Can be a pendulum.Date or date.
  

**Returns**:

  A py date object

