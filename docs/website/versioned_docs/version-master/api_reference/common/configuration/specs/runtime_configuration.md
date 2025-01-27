---
sidebar_label: runtime_configuration
title: common.configuration.specs.runtime_configuration
---

## RuntimeConfiguration Objects

```python
@configspec
class RuntimeConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/runtime_configuration.py#L15)

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

