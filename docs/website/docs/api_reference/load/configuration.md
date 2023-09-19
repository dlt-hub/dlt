---
sidebar_label: configuration
title: load.configuration
---

## LoaderConfiguration Objects

```python
@configspec
class LoaderConfiguration(PoolRunnerConfiguration)
```

#### workers

how many parallel loads can be executed

#### pool\_type

mostly i/o (upload) so may be thread pool

#### raise\_on\_failed\_jobs

when True, raises on terminally failed jobs immediately

#### raise\_on\_max\_retries

When gt 0 will raise when job reaches raise_on_max_retries

