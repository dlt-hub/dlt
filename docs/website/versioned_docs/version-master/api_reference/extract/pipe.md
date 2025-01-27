---
sidebar_label: pipe
title: extract.pipe
---

## ForkPipe Objects

```python
class ForkPipe(ItemTransform[ResolvablePipeItem])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L45)

### \_\_init\_\_

```python
def __init__(pipe: "Pipe", step: int = -1, copy_on_fork: bool = False) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L48)

A transformer that forks the `pipe` and sends the data items to forks added via `add_pipe` method.

## Pipe Objects

```python
class Pipe(SupportsPipe)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L73)

### is\_empty

```python
@property
def is_empty() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L94)

Checks if pipe contains any steps

### is\_data\_bound

```python
@property
def is_data_bound() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L103)

Checks if pipe is bound to data and can be iterated. Pipe is bound if has a parent that is bound xor is not empty.

### gen

```python
@property
def gen() -> TPipeStep
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L111)

A data generating step

### find

```python
def find(*step_type: AnyType) -> int
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L123)

Finds a step with object of type `step_type`

### append\_step

```python
def append_step(step: TPipeStep) -> "Pipe"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L146)

Appends pipeline step. On first added step performs additional verification if step is a valid data generator

### insert\_step

```python
def insert_step(step: TPipeStep, index: int) -> "Pipe"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L172)

Inserts step at a given index in the pipeline. Allows prepending only for transformers

### remove\_step

```python
def remove_step(index: int) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L188)

Removes steps at a given index. Gen step cannot be removed

### replace\_gen

```python
def replace_gen(gen: TPipeStep) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L199)

Replaces data generating step. Assumes that you know what are you doing

### close

```python
def close() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L204)

Closes pipe generator

### full\_pipe

```python
def full_pipe() -> "Pipe"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L213)

Creates a pipe that from the current and all the parent pipes.

### ensure\_gen\_bound

```python
def ensure_gen_bound() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L233)

Verifies that gen step is bound to data

### evaluate\_gen

```python
def evaluate_gen() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L252)

Lazily evaluate gen of the pipe when creating PipeIterator. Allows creating multiple use pipes from generator functions and lists

### bind\_gen

```python
def bind_gen(*args: Any, **kwargs: Any) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/pipe.py#L288)

Finds and wraps with `args` + `kwargs` the callable generating step in the resource pipe and then replaces the pipe gen with the wrapped one

