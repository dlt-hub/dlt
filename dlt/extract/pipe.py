import inspect
import makefun
from copy import copy
from typing import (
    Any,
    AsyncIterator,
    ClassVar,
    Optional,
    Union,
    Callable,
    Iterable,
    Iterator,
    List,
    Tuple,
)

from dlt.common.typing import AnyFun, AnyType, TDataItems
from dlt.common.utils import get_callable_name

from dlt.extract.exceptions import (
    CreatePipeException,
    InvalidStepFunctionArguments,
    InvalidResourceDataTypeFunctionNotAGenerator,
    InvalidTransformerGeneratorFunction,
    ParametrizedResourceUnbound,
    PipeNotBoundToData,
    UnclosablePipe,
)
from dlt.extract.items import (
    ItemTransform,
    ResolvablePipeItem,
    SupportsPipe,
    TPipeStep,
    TPipedDataItems,
)
from dlt.extract.utils import (
    check_compat_transformer,
    simulate_func_call,
    wrap_compat_transformer,
    wrap_resource_gen,
    wrap_async_iterator,
)


class ForkPipe(ItemTransform[ResolvablePipeItem]):
    placement_affinity: ClassVar[float] = 2

    def __init__(self, pipe: "Pipe", step: int = -1, copy_on_fork: bool = False) -> None:
        """A transformer that forks the `pipe` and sends the data items to forks added via `add_pipe` method."""
        self._pipes: List[Tuple["Pipe", int]] = []
        self.copy_on_fork = copy_on_fork
        """If true, the data items going to a forked pipe will be copied"""
        self.add_pipe(pipe, step)

    def add_pipe(self, pipe: "Pipe", step: int = -1) -> None:
        if pipe not in self._pipes:
            self._pipes.append((pipe, step))

    def has_pipe(self, pipe: "Pipe") -> bool:
        return pipe in [p[0] for p in self._pipes]

    def __call__(self, item: TDataItems, meta: Any = None) -> Iterator[ResolvablePipeItem]:
        for i, (pipe, step) in enumerate(self._pipes):
            if i == 0 or not self.copy_on_fork:
                _it = item
            else:
                # shallow copy the item
                _it = copy(item)
            # always start at the beginning
            yield ResolvablePipeItem(_it, step, pipe, meta)


class Pipe(SupportsPipe):
    def __init__(self, name: str, steps: List[TPipeStep] = None, parent: "Pipe" = None) -> None:
        self.name = name
        self._gen_idx = 0
        self._steps: List[TPipeStep] = []
        self.parent = parent
        # add the steps, this will check and mod transformations
        if steps:
            for index, step in enumerate(steps):
                self.insert_step(step, index)

    @classmethod
    def from_data(
        cls,
        name: str,
        gen: Union[Iterable[TPipedDataItems], Iterator[TPipedDataItems], AnyFun],
        parent: "Pipe" = None,
    ) -> "Pipe":
        return cls(name, [gen], parent=parent)

    @property
    def is_empty(self) -> bool:
        """Checks if pipe contains any steps"""
        return len(self._steps) == 0

    @property
    def has_parent(self) -> bool:
        return self.parent is not None

    @property
    def is_data_bound(self) -> bool:
        """Checks if pipe is bound to data and can be iterated. Pipe is bound if has a parent that is bound xor is not empty."""
        if self.has_parent:
            return self.parent.is_data_bound
        else:
            return not self.is_empty

    @property
    def gen(self) -> TPipeStep:
        """A data generating step"""
        return self._steps[self._gen_idx]

    @property
    def tail(self) -> TPipeStep:
        return self._steps[-1]

    @property
    def steps(self) -> List[TPipeStep]:
        return self._steps

    def find(self, *step_type: AnyType) -> int:
        """Finds a step with object of type `step_type`"""
        return next((i for i, v in enumerate(self._steps) if isinstance(v, step_type)), -1)

    def __getitem__(self, i: int) -> TPipeStep:
        return self._steps[i]

    def __len__(self) -> int:
        return len(self._steps)

    def fork(self, child_pipe: "Pipe", child_step: int = -1, copy_on_fork: bool = False) -> "Pipe":
        if len(self._steps) == 0:
            raise CreatePipeException(self.name, f"Cannot fork to empty pipe {child_pipe}")
        fork_step = self.tail
        if not isinstance(fork_step, ForkPipe):
            fork_step = ForkPipe(child_pipe, child_step, copy_on_fork)
            # always add this at the end
            self.insert_step(fork_step, len(self))
        else:
            if not fork_step.has_pipe(child_pipe):
                fork_step.add_pipe(child_pipe, child_step)
        return self

    def append_step(self, step: TPipeStep) -> "Pipe":
        """Appends pipeline step. On first added step performs additional verification if step is a valid data generator"""
        steps_count = len(self._steps)

        if steps_count == 0 and not self.has_parent:
            self._verify_head_step(step)
        else:
            step = self._wrap_transform_step_meta(steps_count, step)

        # find the insert position using particular
        if steps_count > 0:
            affinity = step.placement_affinity if isinstance(step, ItemTransform) else 0
            for index in reversed(range(0, steps_count)):
                step_at_idx = self._steps[index]
                affinity_at_idx = (
                    step_at_idx.placement_affinity if isinstance(step_at_idx, ItemTransform) else 0
                )
                if affinity_at_idx <= affinity:
                    self._insert_at_pos(step, index + 1)
                    return self
            # insert at the start due to strong affinity
            self._insert_at_pos(step, 0)
        else:
            self._steps.append(step)
        return self

    def insert_step(self, step: TPipeStep, index: int) -> "Pipe":
        """Inserts step at a given index in the pipeline. Allows prepending only for transformers"""
        steps_count = len(self._steps)
        if steps_count == 0:
            return self.append_step(step)
        if index == 0:
            if not self.has_parent:
                raise CreatePipeException(
                    self.name,
                    "You cannot insert a step before head of the resource that is not a"
                    " transformer",
                )
        step = self._wrap_transform_step_meta(index, step)
        self._insert_at_pos(step, index)
        return self

    def remove_step(self, index: int) -> None:
        """Removes steps at a given index. Gen step cannot be removed"""
        if index == self._gen_idx:
            raise CreatePipeException(
                self.name,
                f"Step at index {index} holds a data generator for this pipe and cannot be removed",
            )
        self._steps.pop(index)
        if index < self._gen_idx:
            self._gen_idx -= 1

    def replace_gen(self, gen: TPipeStep) -> None:
        """Replaces data generating step. Assumes that you know what are you doing"""
        assert not self.is_empty
        self._steps[self._gen_idx] = gen

    def close(self) -> None:
        """Closes pipe generator"""
        gen = self.gen
        # NOTE: async generator are wrapped in generators
        if inspect.isgenerator(gen):
            gen.close()
        else:
            raise UnclosablePipe(self.name, gen)

    def full_pipe(self) -> "Pipe":
        """Creates a pipe that from the current and all the parent pipes."""
        # prevent creating full pipe with unbound heads
        if self.has_parent:
            self._ensure_transform_step(self._gen_idx, self.gen)
        else:
            self.ensure_gen_bound()

        if self.has_parent:
            steps = self.parent.full_pipe().steps
        else:
            steps = []

        steps.extend(self._steps)
        p = Pipe(self.name, [])
        # set the steps so they are not evaluated again
        p._steps = steps
        # return pipe with resolved dependencies
        return p

    def ensure_gen_bound(self) -> None:
        """Verifies that gen step is bound to data"""
        head = self.gen
        if not callable(head):
            return
        sig = inspect.signature(head)
        try:
            # must bind without arguments
            sig.bind()
        except TypeError as ex:
            callable_name = get_callable_name(head)
            raise ParametrizedResourceUnbound(
                self.name,
                callable_name,
                sig.replace(parameters=list(sig.parameters.values())[1:]),
                "resource",
                str(ex),
            )

    def evaluate_gen(self) -> None:
        """Lazily evaluate gen of the pipe when creating PipeIterator. Allows creating multiple use pipes from generator functions and lists"""
        if not self.is_data_bound:
            raise PipeNotBoundToData(self.name, self.has_parent)

        gen = self.gen
        if not self.has_parent:
            if callable(gen):
                try:
                    # must be parameter-less callable or parameters must have defaults
                    self.replace_gen(gen())  # type: ignore
                except TypeError as ex:
                    raise ParametrizedResourceUnbound(
                        self.name,
                        get_callable_name(gen),
                        inspect.signature(gen),
                        "resource",
                        str(ex),
                    )
            # otherwise it must be an iterator
            if isinstance(gen, Iterable):
                self.replace_gen(iter(gen))
        else:
            # verify if transformer can be called
            self._ensure_transform_step(self._gen_idx, gen)

        # wrap async generator
        if isinstance(self.gen, AsyncIterator):
            self.replace_gen(wrap_async_iterator(self.gen))

        # evaluate transforms
        for step_no, step in enumerate(self._steps):
            # print(f"pipe {self.name} step no {step_no} step({step})")
            if isinstance(step, ItemTransform):
                self._steps[step_no] = step.bind(self)

    def bind_gen(self, *args: Any, **kwargs: Any) -> Any:
        """Finds and wraps with `args` + `kwargs` the callable generating step in the resource pipe and then replaces the pipe gen with the wrapped one"""
        try:
            gen = self._wrap_gen(*args, **kwargs)
            self.replace_gen(gen)
            return gen
        except InvalidResourceDataTypeFunctionNotAGenerator:
            try:
                # call regular function to check what is inside
                _data = self.gen(*args, **kwargs)  # type: ignore
            except Exception as ev_ex:
                # break chaining
                raise ev_ex from None
            # accept if pipe or object holding pipe is returned
            # TODO: use a protocol (but protocols are slow)
            if isinstance(_data, Pipe) or hasattr(_data, "_pipe"):
                return _data
            raise

    def _wrap_gen(self, *args: Any, **kwargs: Any) -> Any:
        """Finds and wraps with `args` + `kwargs` the callable generating step in the resource pipe."""
        head = self.gen
        _data: Any = None

        # skip the data item argument for transformers
        args_to_skip = 1 if self.has_parent else 0
        # simulate function call
        sig, _, _ = simulate_func_call(head, args_to_skip, *args, **kwargs)
        assert callable(head)

        # create wrappers with partial
        if self.has_parent:
            _data = wrap_compat_transformer(self.name, head, sig, *args, **kwargs)
        else:
            _data = wrap_resource_gen(self.name, head, sig, *args, **kwargs)
        return _data

    def _verify_head_step(self, step: TPipeStep) -> None:
        # first element must be Iterable, Iterator or Callable in resource pipe
        if not isinstance(step, (Iterable, Iterator, AsyncIterator)) and not callable(step):
            raise CreatePipeException(
                self.name,
                "A head of a resource pipe must be Iterable, Iterator, AsyncIterator or a Callable",
            )

    def _wrap_transform_step_meta(self, step_no: int, step: TPipeStep) -> TPipeStep:
        # step must be a callable: a transformer or a transformation
        if isinstance(step, (Iterable, Iterator)) and not callable(step):
            if self.has_parent:
                raise CreatePipeException(
                    self.name, "Iterable or Iterator cannot be a step in transformer pipe"
                )
            else:
                raise CreatePipeException(
                    self.name, "Iterable or Iterator can only be a first step in resource pipe"
                )

        if not callable(step):
            raise CreatePipeException(
                self.name,
                "Pipe step must be a callable taking one data item as argument and optional second"
                " meta argument",
            )
        else:
            # check the signature
            sig = inspect.signature(step)
            meta_arg = check_compat_transformer(self.name, step, sig)
            if meta_arg is None:
                # add meta parameter when not present
                orig_step = step

                def _partial(*args: Any, **kwargs: Any) -> Any:
                    # orig step does not have meta
                    kwargs.pop("meta", None)
                    # del kwargs["meta"]
                    return orig_step(*args, **kwargs)

                meta_arg = inspect.Parameter(
                    "meta", inspect._ParameterKind.KEYWORD_ONLY, default=None
                )
                kwargs_arg = next(
                    (p for p in sig.parameters.values() if p.kind == inspect.Parameter.VAR_KEYWORD),
                    None,
                )
                if kwargs_arg:
                    # pass meta in variadic
                    new_sig = sig
                else:
                    new_sig = makefun.add_signature_parameters(sig, last=(meta_arg,))
                step = makefun.wraps(step, new_sig=new_sig)(_partial)

            # verify the step callable, gen may be parametrized and will be evaluated at run time
            if not self.is_empty:
                self._ensure_transform_step(step_no, step)
        return step

    def _ensure_transform_step(self, step_no: int, step: TPipeStep) -> None:
        """Verifies that `step` is a valid callable to be a transform step of the pipeline"""
        assert callable(step), f"{step} must be callable"

        sig = inspect.signature(step)
        try:
            # get eventually modified sig
            sig.bind("item", meta="meta")
        except TypeError as ty_ex:
            callable_name = get_callable_name(step)
            if step_no == self._gen_idx:
                # error for gen step
                if len(sig.parameters) == 0:
                    raise InvalidTransformerGeneratorFunction(self.name, callable_name, sig, code=1)
                else:
                    # show the sig without first argument
                    raise ParametrizedResourceUnbound(
                        self.name,
                        callable_name,
                        sig.replace(parameters=list(sig.parameters.values())[1:]),
                        "transformer",
                        str(ty_ex),
                    )
            else:
                raise InvalidStepFunctionArguments(self.name, callable_name, sig, str(ty_ex))

    def _clone(self, new_name: str = None, with_parent: bool = False) -> "Pipe":
        """Clones the pipe steps, optionally renaming the pipe. Used internally to clone a list of connected pipes."""
        new_parent = self.parent
        if with_parent and self.parent and not self.parent.is_empty:
            parent_new_name = new_name
            if new_name:
                # if we are renaming the pipe, then also rename the parent
                if self.name in self.parent.name:
                    parent_new_name = self.parent.name.replace(self.name, new_name)
                else:
                    parent_new_name = f"{self.parent.name}_{new_name}"
            new_parent = self.parent._clone(parent_new_name, with_parent)

        p = Pipe(new_name or self.name, [], new_parent)
        p._steps = self._steps.copy()
        return p

    def _insert_at_pos(self, step: Any, index: int) -> None:
        # shift right if no parent
        if index == 0 and not self.has_parent:
            # put after gen
            index += 1
        # actually insert in the list
        self._steps.insert(index, step)
        # increase the _gen_idx if added before generator
        if index <= self._gen_idx:
            self._gen_idx += 1

    def __repr__(self) -> str:
        if self.has_parent:
            bound_str = " data bound to " + repr(self.parent)
        else:
            bound_str = ""
        return f"Pipe {self.name} [steps: {len(self._steps)}] at {id(self)}{bound_str}"
