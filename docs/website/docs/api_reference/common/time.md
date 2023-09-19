---
sidebar_label: time
title: common.time
---

#### timestamp\_within

```python
def timestamp_within(timestamp: float, min_exclusive: Optional[float],
                     max_inclusive: Optional[float]) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/time.py#L16)

check if timestamp within range uniformly treating none and range inclusiveness

#### timestamp\_before

```python
def timestamp_before(timestamp: float, max_inclusive: Optional[float]) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/time.py#L23)

check if timestamp is before max timestamp, inclusive

#### ensure\_pendulum\_date

```python
def ensure_pendulum_date(value: TAnyDateTime) -> pendulum.Date
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/time.py#L48)

Coerce a date/time value to a `pendulum.Date` object.

UTC is assumed if the value is not timezone aware. Other timezones are shifted to UTC

**Arguments**:

- `value` - The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.
  

**Returns**:

  A timezone aware pendulum.Date object.

#### ensure\_pendulum\_datetime

```python
def ensure_pendulum_datetime(value: TAnyDateTime) -> pendulum.DateTime
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/time.py#L75)

Coerce a date/time value to a `pendulum.DateTime` object.

UTC is assumed if the value is not timezone aware. Other timezones are shifted to UTC

**Arguments**:

- `value` - The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.
  

**Returns**:

  A timezone aware pendulum.DateTime object in UTC timezone.

#### ensure\_pendulum\_time

```python
def ensure_pendulum_time(value: Union[str, datetime.time]) -> pendulum.Time
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/time.py#L102)

Coerce a time value to a `pendulum.Time` object.

**Arguments**:

- `value` - The value to coerce. Can be a `pendulum.Time` / `datetime.time` or an iso time string.
  

**Returns**:

  A pendulum.Time object

