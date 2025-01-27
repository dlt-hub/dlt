---
sidebar_label: session
title: sources.helpers.requests.session
---

## Session Objects

```python
class Session(BaseSession)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/requests/session.py#L24)

Requests session which by default adds a timeout to all requests and calls `raise_for_status()` on response

**Arguments**:

- `timeout` - Timeout for requests in seconds. May be passed as `timedelta` or `float/int` number of seconds.
  May be a single value or a tuple for separate (connect, read) timeout.
- `raise_for_status` - Whether to raise exception on error status codes (using `response.raise_for_status()`)

