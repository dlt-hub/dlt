---
sidebar_label: retry
title: sources.helpers.requests.retry
---

## retry\_if\_status Objects

```python
class retry_if_status(retry_base)
```

Retry for given response status codes

## Client Objects

```python
class Client()
```

Wrapper for `requests` to create a `Session` with configurable retry functionality.

### Summary
Create a  `requests.Session` which automatically retries requests in case of error.
By default retries are triggered for `5xx` and `429` status codes and when the server is unreachable or drops connection.

### Custom retry condition
You can provide one or more custom predicates for specific retry condition. The predicate is called after every request with the resulting response and/or exception.
For example, this will trigger a retry when the response text is `error`:

>>> from typing import Optional
>>> from requests import Response
>>>
>>> def should_retry(response: Optional[Response], exception: Optional[BaseException]) -> bool:
>>>     if response is None:
>>>         return False
>>>     return response.text == 'error'

The retry is triggered when either any of the predicates or the default conditions based on status code/exception are `True`.

### Args:
request_timeout: Timeout for requests in seconds. May be passed as `timedelta` or `float/int` number of seconds.
max_connections: Max connections per host in the HTTPAdapter pool
raise_for_status: Whether to raise exception on error status codes (using `response.raise_for_status()`)
session: Optional `requests.Session` instance to add the retry handler to. A new session is created by default.
status_codes: Retry when response has any of these status codes. Default `429` and all `5xx` codes. Pass an empty list to disable retry based on status.
exceptions: Retry on exception of given type(s). Default `(requests.Timeout, requests.ConnectionError)`. Pass an empty list to disable retry on exceptions.
request_max_attempts: Max number of retry attempts before giving up
retry_condition: A predicate or a list of predicates to decide whether to retry. If any predicate returns `True` the request is retried
request_backoff_factor: Multiplier used for exponential delay between retries
request_max_retry_delay: Maximum delay when using exponential backoff
respect_retry_after_header: Whether to use the `Retry-After` response header (when available) to determine the retry delay
session_attrs: Extra attributes that will be set on the session instance, e.g. `{headers: {'Authorization': 'api-key'}}` (see `requests.sessions.Session` for possible attributes)

#### update\_from\_config

```python
def update_from_config(config: RunConfiguration) -> None
```

Update session/retry settings from RunConfiguration

