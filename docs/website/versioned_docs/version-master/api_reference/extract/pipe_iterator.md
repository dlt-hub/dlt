---
sidebar_label: pipe_iterator
title: extract.pipe_iterator
---

## PipeIterator Objects

```python
class PipeIterator(Iterator[PipeItem])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe_iterator.py#L47)

### clone\_pipes

```python
@staticmethod
def clone_pipes(
    pipes: Sequence[Pipe],
    existing_cloned_pairs: Dict[int, Pipe] = None
) -> Tuple[List[Pipe], Dict[int, Pipe]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe_iterator.py#L331)

This will clone pipes and fix the parent/dependent references

## ManagedPipeIterator Objects

```python
class ManagedPipeIterator(PipeIterator)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe_iterator.py#L361)

A version of the pipe iterator that gets closed automatically on an exception in _next_

### set\_context

```python
def set_context(ctx: List[ContainerInjectableContext]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe_iterator.py#L367)

Sets list of injectable contexts that will be injected into Container for each call to __next__

