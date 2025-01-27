---
sidebar_label: concurrency
title: extract.concurrency
---

## FuturesPool Objects

```python
class FuturesPool()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/concurrency.py#L23)

Worker pool for pipe items that can be resolved asynchronously.

Items can be either asyncio coroutines or regular callables which will be executed in a thread pool.

### submit

```python
def submit(pipe_item: ResolvablePipeItem) -> TItemFuture
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/concurrency.py#L88)

Submit an item to the pool.

**Arguments**:

- `pipe_item` - The pipe item to submit. `pipe_item.item` must be either an asyncio coroutine or a callable.
  

**Returns**:

  The resulting future object

### resolve\_next\_future

```python
def resolve_next_future(
        use_configured_timeout: bool = False) -> Optional[ResolvablePipeItem]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/concurrency.py#L157)

Block until the next future is done and return the result. Returns None if no futures done.

**Arguments**:

- `use_configured_timeout` - If True, use the value of `self.poll_interval` as the max wait time,
  raises `concurrent.futures.TimeoutError` if no future is done within that time.
  

**Returns**:

  The resolved future item or None if no future is done.

### resolve\_next\_future\_no\_wait

```python
def resolve_next_future_no_wait() -> Optional[ResolvablePipeItem]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/concurrency.py#L186)

Resolve the first done future in the pool.
This does not block and returns None if no future is done.

