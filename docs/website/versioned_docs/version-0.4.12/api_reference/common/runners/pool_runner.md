---
sidebar_label: pool_runner
title: common.runners.pool_runner
---

## NullExecutor Objects

```python
class NullExecutor(Executor)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/runners/pool_runner.py#L22)

Dummy executor that runs jobs single-threaded.

Provides a uniform interface for `None` pool type

### submit

```python
def submit(fn: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> Future[T]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/runners/pool_runner.py#L28)

Run the job and return a Future

