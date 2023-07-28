---
title: Performance
description: Scale-up, parallelize and finetune dlt pipelines
keywords: [scaling, parallelism, finetuning]
---

# Performance

## Yield pages instead of rows

If you can, yield pages when producing data. This makes some processes more effective by lowering
the necessary function calls.

## Memory/disk management

### Controlling in-memory and filesystem buffers
`dlt` likes resources that yield data because it can request data into a buffer before processing
and releasing it. This makes it possible to manage the amount of resources used. In order to
configure this option, you can specify buffer size via env variables or by adding to the
`config.toml`.

Globally in your `config.toml`:

```toml
[data_writer]
max_buffer_items=100
```

or specifically for the normalization and source:

```toml
[normalize.data_writer]
max_buffer_items=100

[sources.data_writer]
max_buffer_items=200
```

The default buffer is actually set to a moderately low value, so unless you are trying to run `dlt`
on IOT sensors or other tiny infrastructures, you might actually want to increase it to speed up
processing.

### Disabling and enabling file compression
Several [text file formats](../dlt-ecosystem/file-formats/) have `gzip` compression enabled by default. If you wish that your load packages have uncompressed files (ie. to debug the content easily), change `data_writer.disable_compression` in config.toml. The entry below will disable the compression of the files processed in `normalize` stage.
```toml
[normalize.data_writer]
disable_compression=false
```


### Freeing disk space after loading

Keep in mind load packages are buffered to disk and are left for any troubleshooting, so you can [clear disk space by setting `delete_completed_jobs` option](../running-in-production/running.md#data-left-behind).

### Observing cpu and memory usage
Please make sure that you have `psutils` package installed (note that Airflow installs it by default). Then you can dump the stats periodically by setting the [progress](../general-usage/pipeline.md#display-the-loading-progress) to `log` in `config.toml`:
```toml
progress="log"
```
or when running the pipeline:
```sh
PROGRESS=log python pipeline_script.py
```

## Parallelism

Parallelism can be limited with the config option `max_parallel_items = 5` that you can place under
a source. As `dlt` is a library can also leverage parallelism outside of `dlt` such as by placing
tasks in parallel in a dag.

```toml
[extract] # global setting
max_parallel_items=5

[sources.my_pipeline.extract] # setting for the "my_pipeline" pipeline
max_parallel_items=5
```

## Resources loading, `fifo` vs. `round robin`

When extracting from resources, you have two options to determine what the order of queries to your
resources are: `fifo` and `round_robin`.

`fifo` is the default option and will result in every resource being fully extracted before the next
resource is extracted in the order that you added them to your source.

`round_robin` will result in extraction of one item from the first resource, then one item from the
second resource etc, doing as many rounds as necessary until all resources are fully extracted.

You can change this setting in your `config.toml` as follows:

```toml
[extract] # global setting
next_item_mode=round_robin

[sources.my_pipeline.extract] # setting for the "my_pipeline" pipeline
next_item_mode=5
```

## Using the built in requests client

`dlt` provides a customized [requests](https://requests.readthedocs.io/en/latest/) client with automatic retries and configurable timeouts.

We recommend using this to make API calls in your sources as it makes your pipeline more resilient to intermittent network errors and other random glitches which otherwise can cause the whole pipeline to fail.

For most use cases this is a drop in replacement for `requests`, so:

:heavy_multiplication_x: **Don't**

```python
import requests
```
:heavy_check_mark: **Do**

```python
from dlt.sources.helpers import requests
```

And use it just like you would use `requests`:

```python
response = requests.get('https://example.com/api/contacts', headers={'Authorization': MY_API_KEY})
data = response.json()
...
```

### Retry rules

By default failing requests are retried up to 5 times with an exponentially increasing delay. That means the first retry will wait 1 second and the fifth retry will wait 16 seconds.

If all retry attempts fail the corresponding requests exception is raised. E.g. `requests.HTTPError` or `requests.ConnectionTimeout`

All standard HTTP server errors trigger a retry. This includes:

* Error status codes:

    All status codes in the `500` range and `429` (too many requests).
    Commonly servers include a `Retry-After` header with `429` and `503` responses.
    When detected this value supersedes the standard retry delay.

* Connection and timeout errors

    When the remote server is unreachable, connection is unexpectedly dropped or when the request takes longer than the configured `timeout`.

### Customizing retry settings


Many requests settings can be added to the runtime section in your `config.toml`. For example:

```toml
[runtime]
request_max_attempts = 10  # Stop after 10 retry attempts instead of 5
request_backoff_factor = 1.5  # Multiplier applied to the exponential delays. Default is 1
request_timeout = 120  # Timeout in seconds
request_max_retry_delay = 30  # Cap exponential delay to 30 seconds
```

For more control you can create your own instance of `dlt.sources.requests.Client` and use that instead of the global client.

This lets you customize which status codes and exceptions to retry on:

```python
from dlt.sources.helpers import requests

http_client = requests.Client(
    status_codes=(403, 500, 502, 503),
    exceptions=(requests.ConnectionError, requests.ChunkedEncodingError)
)
```

and you may even supply a custom retry condition in the form of a predicate.
This is sometimes needed when loading from non-standard APIs which don't use HTTP error codes.

For example:

```python
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

