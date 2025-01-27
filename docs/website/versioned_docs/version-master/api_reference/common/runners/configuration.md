---
sidebar_label: configuration
title: common.runners.configuration
---

## PoolRunnerConfiguration Objects

```python
@configspec
class PoolRunnerConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runners/configuration.py#L10)

### pool\_type

type of pool to run, must be set in derived configs

### start\_method

start method for the pool (typically process). None is system default

### workers

__how many threads/processes in the pool__


### run\_sleep

how long to sleep between runs with workload, seconds

