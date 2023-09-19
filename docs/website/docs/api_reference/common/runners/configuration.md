---
sidebar_label: configuration
title: common.runners.configuration
---

## PoolRunnerConfiguration Objects

```python
@configspec
class PoolRunnerConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runners/configuration.py#L10)

#### pool\_type

type of pool to run, must be set in derived configs

#### workers

how many threads/processes in the pool

#### run\_sleep

how long to sleep between runs with workload, seconds

