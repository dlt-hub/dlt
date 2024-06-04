import inspect
import makefun
import asyncio
from typing import (
    Callable,
    Optional,
    Tuple,
    Union,
    List,
    Any,
    Sequence,
    cast,
    AsyncIterator,
    AsyncGenerator,
    Awaitable,
    Generator,
    Iterator,
)
from collections.abc import Mapping as C_Mapping
from functools import wraps, partial

from dlt.common.data_writers import TDataItemFormat
from dlt.common.exceptions import MissingDependencyException
from dlt.common.pipeline import reset_resource_state
from dlt.common.schema.typing import TColumnNames, TAnySchemaColumns, TTableSchemaColumns
from dlt.common.typing import AnyFun, DictStrAny, TDataItem, TDataItems, TAnyFunOrGenerator
from dlt.common.utils import get_callable_name

from dlt.extract.exceptions import (
    InvalidResourceDataTypeFunctionNotAGenerator,
    InvalidStepFunctionArguments,
)
from dlt.extract.items import (
    TTableHintTemplate,
    TDataItem,
    TFunHintTemplate,
    SupportsPipe,
)

try:
    from dlt.common.libs import pydantic
except MissingDependencyException:
    pydantic = None


try:
    from dlt.common.libs import pyarrow
except MissingDependencyException:
    pyarrow = None

try:
    from dlt.common.libs.pandas import pandas
except MissingDependencyException:
    pandas = None


def get_data_item_format(items: TDataItems) -> TDataItemFormat:
    """Detect the format of the data item from `items`.

    Reverts to `object` for empty lists

    Returns:
        The data file format.
    """
    if not pyarrow and not pandas:
        return "object"

    # Assume all items in list are the same type
    try:
        if isinstance(items, list):
            items = items[0]
        if (pyarrow and pyarrow.is_arrow_item(items)) or (
            pandas and isinstance(items, pandas.DataFrame)
        ):
            return "arrow"
    except IndexError:
        pass
    return "object"


def resolve_column_value(
    column_hint: TTableHintTemplate[TColumnNames], item: TDataItem
) -> Union[Any, List[Any]]:
    """Extract values from the data item given a column hint.
    Returns either a single value or list of values when hint is a composite.
    """
    columns = column_hint(item) if callable(column_hint) else column_hint
    if isinstance(columns, str):
        return item[columns]
    return [item[k] for k in columns]


def ensure_table_schema_columns(columns: TAnySchemaColumns) -> TTableSchemaColumns:
    """Convert supported column schema types to a column dict which
    can be used in resource schema.

    Args:
        columns: A dict of column schemas, a list of column schemas, or a pydantic model
    """
    if isinstance(columns, C_Mapping):
        # fill missing names in short form was used
        for col_name in columns:
            columns[col_name]["name"] = col_name
        return columns
    elif isinstance(columns, Sequence):
        # Assume list of columns
        return {col["name"]: col for col in columns}
    elif pydantic is not None and (
        isinstance(columns, pydantic.BaseModel) or issubclass(columns, pydantic.BaseModel)
    ):
        return pydantic.pydantic_to_table_schema_columns(columns)

    raise ValueError(f"Unsupported columns type: {type(columns)}")


def ensure_table_schema_columns_hint(
    columns: TTableHintTemplate[TAnySchemaColumns],
) -> TTableHintTemplate[TTableSchemaColumns]:
    """Convert column schema hint to a hint returning `TTableSchemaColumns`.
    A callable hint is wrapped in another function which converts the original result.
    """
    if callable(columns) and not isinstance(columns, type):

        def wrapper(item: TDataItem) -> TTableSchemaColumns:
            return ensure_table_schema_columns(
                cast(TFunHintTemplate[TAnySchemaColumns], columns)(item)
            )

        return wrapper

    return ensure_table_schema_columns(columns)


def reset_pipe_state(pipe: SupportsPipe, source_state_: Optional[DictStrAny] = None) -> None:
    """Resets the resource state for a `pipe` and all its parent pipes"""
    if pipe.has_parent:
        reset_pipe_state(pipe.parent, source_state_)
    reset_resource_state(pipe.name, source_state_)


def simulate_func_call(
    f: Union[Any, AnyFun], args_to_skip: int, *args: Any, **kwargs: Any
) -> Tuple[inspect.Signature, inspect.Signature, inspect.BoundArguments]:
    """Simulates a call to a resource or transformer function before it will be wrapped for later execution in the pipe

    Returns a tuple with a `f` signature, modified signature in case of transformers and bound arguments
    """
    if not callable(f):
        # just provoke a call to raise default exception
        f()
    assert callable(f)

    sig = inspect.signature(f)
    # simulate the call to the underlying callable
    no_item_sig = sig.replace(parameters=list(sig.parameters.values())[args_to_skip:])
    try:
        bound_args = no_item_sig.bind(*args, **kwargs)
    except TypeError as v_ex:
        raise TypeError(f"{get_callable_name(f)}(): " + str(v_ex))
    return sig, no_item_sig, bound_args


def check_compat_transformer(name: str, f: AnyFun, sig: inspect.Signature) -> inspect.Parameter:
    sig_arg_count = len(sig.parameters)
    callable_name = get_callable_name(f)
    if sig_arg_count == 0:
        raise InvalidStepFunctionArguments(name, callable_name, sig, "Function takes no arguments")

    # see if meta is present in kwargs
    meta_arg = next((p for p in sig.parameters.values() if p.name == "meta"), None)
    if meta_arg is not None:
        if meta_arg.kind not in (meta_arg.KEYWORD_ONLY, meta_arg.POSITIONAL_OR_KEYWORD):
            raise InvalidStepFunctionArguments(
                name, callable_name, sig, "'meta' cannot be pos only argument '"
            )
    return meta_arg


def wrap_async_iterator(
    gen: AsyncIterator[TDataItems],
) -> Generator[Awaitable[TDataItems], None, None]:
    """Wraps an async generator into a list of awaitables"""
    exhausted = False
    busy = False

    # creates an awaitable that will return the next item from the async generator
    async def run() -> TDataItems:
        nonlocal exhausted
        try:
            # if marked exhausted by the main thread and we are wrapping a generator
            # we can close it here
            if exhausted:
                raise StopAsyncIteration()
            item = await gen.__anext__()
            return item
        # on stop iteration mark as exhausted
        # also called when futures are cancelled
        except StopAsyncIteration:
            exhausted = True
            raise
        finally:
            nonlocal busy
            busy = False

    # this generator yields None while the async generator is not exhausted
    try:
        while not exhausted:
            while busy:
                yield None
            busy = True
            yield run()
    # this gets called from the main thread when the wrapping generater is closed
    except GeneratorExit:
        # mark as exhausted
        exhausted = True


def wrap_parallel_iterator(f: TAnyFunOrGenerator) -> TAnyFunOrGenerator:
    """Wraps a generator for parallel extraction"""

    def _gen_wrapper(*args: Any, **kwargs: Any) -> Iterator[TDataItems]:
        gen: TAnyFunOrGenerator
        if callable(f):
            gen = f(*args, **kwargs)
        else:
            gen = f

        exhausted = False
        busy = False

        def _parallel_gen() -> TDataItems:
            nonlocal busy
            nonlocal exhausted
            try:
                return next(gen)  # type: ignore[call-overload]
            except StopIteration:
                exhausted = True
                return None
            finally:
                busy = False

        while not exhausted:
            try:
                while busy:
                    yield None
                busy = True
                yield _parallel_gen
            except GeneratorExit:
                gen.close()  # type: ignore[attr-defined]
                raise

    if callable(f):
        if inspect.isgeneratorfunction(inspect.unwrap(f)):
            return wraps(f)(_gen_wrapper)  # type: ignore[return-value]
        else:

            def _fun_wrapper(*args: Any, **kwargs: Any) -> Any:
                def _curry() -> Any:
                    return f(*args, **kwargs)

                return _curry

            return wraps(f)(_fun_wrapper)  # type: ignore[return-value]
    return _gen_wrapper()  # type: ignore[return-value]


def wrap_compat_transformer(
    name: str, f: AnyFun, sig: inspect.Signature, *args: Any, **kwargs: Any
) -> AnyFun:
    """Creates a compatible wrapper over transformer function. A pure transformer function expects data item in first argument and one keyword argument called `meta`"""
    check_compat_transformer(name, f, sig)
    if len(sig.parameters) == 2 and "meta" in sig.parameters:
        return f

    def _tx_partial(item: TDataItems, meta: Any = None) -> Any:
        # print(f"_ITEM:{item}{meta},{args}{kwargs}")
        # also provide optional meta so pipe does not need to update arguments
        if "meta" in kwargs:
            kwargs["meta"] = meta
        return f(item, *args, **kwargs)

    # this partial wraps transformer and sets a signature that is compatible with pipe transform calls
    return makefun.wraps(f, new_sig=inspect.signature(_tx_partial))(_tx_partial)  # type: ignore


def wrap_resource_gen(
    name: str, f: AnyFun, sig: inspect.Signature, *args: Any, **kwargs: Any
) -> AnyFun:
    """Wraps a generator or generator function so it is evaluated on extraction"""

    if (
        inspect.isgeneratorfunction(inspect.unwrap(f))
        or inspect.isgenerator(f)
        or inspect.isasyncgenfunction(inspect.unwrap(f))
    ):

        def _partial() -> Any:
            # print(f"_PARTIAL: {args} {kwargs}")
            return f(*args, **kwargs)

        # this partial preserves the original signature and just defers the call to pipe
        return makefun.wraps(f, new_sig=inspect.signature(_partial))(_partial)  # type: ignore
    else:
        raise InvalidResourceDataTypeFunctionNotAGenerator(name, f, type(f))
