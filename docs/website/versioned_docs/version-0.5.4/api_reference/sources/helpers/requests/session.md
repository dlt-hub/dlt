---
sidebar_label: session
title: sources.helpers.requests.session
---

## Session Objects

```python
class Session(BaseSession)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/requests/session.py#L24)

Requests session which by default adds a timeout to all requests and calls `raise_for_status()` on response

**Arguments**:

- `timeout` - Timeout for requests in seconds. May be passed as `timedelta` or `float/int` number of seconds.
  May be a single value or a tuple for separate (connect, read) timeout.
- `raise_for_status` - Whether to raise exception on error status codes (using `response.raise_for_status()`)

