---
sidebar_label: run_configuration
title: common.configuration.specs.run_configuration
---

## RunConfiguration Objects

```python
@configspec
class RunConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/run_configuration.py#L13)

### sentry\_dsn

keep None to disable Sentry

### dlthub\_telemetry

enable or disable dlthub telemetry

### request\_timeout

Timeout for http requests

### request\_max\_attempts

Max retry attempts for http clients

### request\_backoff\_factor

Multiplier applied to exponential retry delay for http requests

### request\_max\_retry\_delay

Maximum delay between http request retries

### config\_files\_storage\_path

Platform connection

