---
sidebar_label: pool_runner
title: common.runners.pool_runner
---

## NullExecutor Objects

```python
class NullExecutor(Executor)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/runners/pool_runner.py#L21)

Dummy executor that runs jobs single-threaded.

Provides a uniform interface for `None` pool type

### submit

```python
def submit(fn: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> Future[T]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/runners/pool_runner.py#L27)

Run the job and return a Future

