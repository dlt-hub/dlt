---
sidebar_label: session
title: sources.helpers.requests.session
---

## Session Objects

```python
class Session(BaseSession)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/sources/helpers/requests/session.py#L21)

Requests session which by default adds a timeout to all requests and calls `raise_for_status()` on response

### Args
    timeout: Timeout for requests in seconds. May be passed as `timedelta` or `float/int` number of seconds.
        May be a single value or a tuple for separate (connect, read) timeout.
    raise_for_status: Whether to raise exception on error status codes (using `response.raise_for_status()`)

