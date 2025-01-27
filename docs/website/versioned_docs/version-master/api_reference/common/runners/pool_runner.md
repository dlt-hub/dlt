---
sidebar_label: pool_runner
title: common.runners.pool_runner
---

## NullExecutor Objects

```python
class NullExecutor(Executor)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runners/pool_runner.py#L23)

Dummy executor that runs jobs single-threaded.

Provides a uniform interface for `None` pool type

### submit

```python
def submit(fn: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> Future[T]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runners/pool_runner.py#L29)

Run the job and return a Future

