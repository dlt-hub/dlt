---
sidebar_label: pipe
title: extract.pipe
---

## ForkPipe Objects

```python
class ForkPipe()
```

#### \_\_init\_\_

```python
def __init__(pipe: "Pipe", step: int = -1, copy_on_fork: bool = False) -> None
```

A transformer that forks the `pipe` and sends the data items to forks added via `add_pipe` method.

## Pipe Objects

```python
class Pipe(SupportsPipe)
```

#### is\_empty

```python
@property
def is_empty() -> bool
```

Checks if pipe contains any steps

#### has\_parent

```python
@property
def has_parent() -> bool
```

Checks if pipe is connected to parent pipe from which it takes data items. Connected pipes are created from transformer resources

#### is\_data\_bound

```python
@property
def is_data_bound() -> bool
```

Checks if pipe is bound to data and can be iterated. Pipe is bound if has a parent that is bound xor is not empty.

#### gen

```python
@property
def gen() -> TPipeStep
```

A data generating step

#### find

```python
def find(*step_type: AnyType) -> int
```

Finds a step with object of type `step_type`

#### append\_step

```python
def append_step(step: TPipeStep) -> "Pipe"
```

Appends pipeline step. On first added step performs additional verification if step is a valid data generator

#### insert\_step

```python
def insert_step(step: TPipeStep, index: int) -> "Pipe"
```

Inserts step at a given index in the pipeline. Allows prepending only for transformers

#### remove\_step

```python
def remove_step(index: int) -> None
```

Removes steps at a given index. Gen step cannot be removed

#### replace\_gen

```python
def replace_gen(gen: TPipeStep) -> None
```

Replaces data generating step. Assumes that you know what are you doing

#### full\_pipe

```python
def full_pipe() -> "Pipe"
```

Creates a pipe that from the current and all the parent pipes.

#### ensure\_gen\_bound

```python
def ensure_gen_bound() -> None
```

Verifies that gen step is bound to data

#### evaluate\_gen

```python
def evaluate_gen() -> None
```

Lazily evaluate gen of the pipe when creating PipeIterator. Allows creating multiple use pipes from generator functions and lists

#### bind\_gen

```python
def bind_gen(*args: Any, **kwargs: Any) -> Any
```

Finds and wraps with `args` + `kwargs` the callable generating step in the resource pipe and then replaces the pipe gen with the wrapped one

## PipeIterator Objects

```python
class PipeIterator(Iterator[PipeItem])
```

#### clone\_pipes

```python
@staticmethod
def clone_pipes(pipes: Sequence[Pipe]) -> List[Pipe]
```

This will clone pipes and fix the parent/dependent references

## ManagedPipeIterator Objects

```python
class ManagedPipeIterator(PipeIterator)
```

A version of the pipe iterator that gets closed automatically on an exception in _next_

#### set\_context

```python
def set_context(ctx: List[ContainerInjectableContext]) -> None
```

Sets list of injectable contexts that will be injected into Container for each call to __next__

