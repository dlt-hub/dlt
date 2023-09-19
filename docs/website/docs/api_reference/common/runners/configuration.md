---
sidebar_label: configuration
title: common.runners.configuration
---

## PoolRunnerConfiguration Objects

```python
@configspec
class PoolRunnerConfiguration(BaseConfiguration)
```

#### pool\_type

type of pool to run, must be set in derived configs

#### workers

how many threads/processes in the pool

#### run\_sleep

how long to sleep between runs with workload, seconds

