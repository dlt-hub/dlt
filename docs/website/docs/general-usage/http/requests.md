---
title: Requests wrapper
description: Use the dlt requests wrapper to make HTTP requests with automatic retries and timeouts
keywords: [http, requests, retry, timeout]
---

`dlt` provides a customized [Python Requests](https://requests.readthedocs.io/en/latest/) client with automatic retries and configurable timeouts.

We recommend using this to make API calls in your sources as it makes your pipeline more resilient to intermittent network errors and other random glitches which otherwise can cause the whole pipeline to fail.

The dlt requests client will additionally set the default user-agent header to `dlt/{DLT_VERSION_NAME}`.

For most use cases, this is a drop-in replacement for `requests`, so in places where you would normally do:

```py
import requests
```

You can instead do:

```py
from dlt.sources.helpers import requests
```

And use it just like you would use `requests`:

```py
response = requests.get(
    'https://example.com/api/contacts',
    headers={'Authorization': MY_API_KEY}
)
data = response.json()
...
```

## Retry rules

By default, failing requests are retried up to 5 times with an exponentially increasing delay. That means the first retry will wait 1 second, and the fifth retry will wait 16 seconds.

If all retry attempts fail, the corresponding requests exception is raised. E.g., `requests.HTTPError` or `requests.ConnectionTimeout`.

All standard HTTP server errors trigger a retry. This includes:

* Error status codes:

    All status codes in the `500` range and `429` (too many requests).
    Commonly, servers include a `Retry-After` header with `429` and `503` responses.
    When detected, this value supersedes the standard retry delay.

* Connection and timeout errors

    When the remote server is unreachable, the connection is unexpectedly dropped, or when the request takes longer than the configured `timeout`.

## Customizing retry settings

Many requests settings can be added to the runtime section in your `config.toml`. For example:

```toml
[runtime]
request_max_attempts = 10  # Stop after 10 retry attempts instead of 5
request_backoff_factor = 1.5  # Multiplier applied to the exponential delays. Default is 1
request_timeout = 120  # Timeout in seconds
request_max_retry_delay = 30  # Cap exponential delay to 30 seconds
```

For more control, you can create your own instance of `dlt.sources.requests.Client` and use that instead of the global client.

This lets you customize which status codes and exceptions to retry on:

```py
from dlt.sources.helpers import requests

http_client = requests.Client(
    status_codes=(403, 500, 502, 503),
    exceptions=(requests.ConnectionError, requests.ChunkedEncodingError)
)
```

and you may even supply a custom retry condition in the form of a predicate.
This is sometimes needed when loading from non-standard APIs which don't use HTTP error codes.

For example:

```py
from dlt.sources.helpers import requests

def retry_if_error_key(response: Optional[requests.Response], exception: Optional[BaseException]) -> bool:
    """Decide whether to retry the request based on whether
    the json response contains an `error` key
    """
    if response is None:
        # Fall back on the default exception predicate.
        return False
    data = response.json()
    return 'error' in data

http_client = Client(
    retry_condition=retry_if_error_key
)
```

