import inspect
import makefun
from typing import Optional, Union, List, Any, Sequence, cast
from collections.abc import Mapping as C_Mapping

from dlt.common.exceptions import MissingDependencyException
from dlt.common.pipeline import reset_resource_state
from dlt.common.schema.typing import TColumnNames, TAnySchemaColumns, TTableSchemaColumns
from dlt.common.typing import AnyFun, DictStrAny, TDataItem, TDataItems
from dlt.common.utils import get_callable_name
from dlt.extract.exceptions import InvalidResourceDataTypeFunctionNotAGenerator

from dlt.extract.typing import TTableHintTemplate, TDataItem, TFunHintTemplate, SupportsPipe

try:
    from dlt.common.libs import pydantic
except MissingDependencyException:
    pydantic = None


def resolve_column_value(column_hint: TTableHintTemplate[TColumnNames], item: TDataItem) -> Union[Any, List[Any]]:
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
        return {col['name']: col for col in columns}
    elif pydantic is not None and (
        isinstance(columns, pydantic.BaseModel) or issubclass(columns, pydantic.BaseModel)
    ):
        return pydantic.pydantic_to_table_schema_columns(columns)

    raise ValueError(f"Unsupported columns type: {type(columns)}")


def ensure_table_schema_columns_hint(columns: TTableHintTemplate[TAnySchemaColumns]) -> TTableHintTemplate[TTableSchemaColumns]:
    """Convert column schema hint to a hint returning `TTableSchemaColumns`.
    A callable hint is wrapped in another function which converts the original result.
    """
    if callable(columns) and not isinstance(columns, type):
        def wrapper(item: TDataItem) -> TTableSchemaColumns:
            return ensure_table_schema_columns(cast(TFunHintTemplate[TAnySchemaColumns], columns)(item))
        return wrapper

    return ensure_table_schema_columns(columns)


def reset_pipe_state(pipe: SupportsPipe, source_state_: Optional[DictStrAny] = None) -> None:
    """Resets the resource state for a `pipe` and all its parent pipes"""
    if pipe.has_parent:
        reset_pipe_state(pipe.parent, source_state_)
    reset_resource_state(pipe.name, source_state_)


def simulate_func_call(f: Union[Any, AnyFun], args_to_skip: int, *args: Any, **kwargs: Any) -> inspect.Signature:
    """Simulates a call to a resource or transformer function before it will be wrapped for later execution in the pipe"""
    if not callable(f):
        # just provoke a call to raise default exception
        f()
    assert callable(f)

    sig = inspect.signature(f)
    # simulate the call to the underlying callable
    if args or kwargs:
        no_item_sig = sig.replace(parameters=list(sig.parameters.values())[args_to_skip:])
        try:
            no_item_sig.bind(*args, **kwargs)
        except TypeError as v_ex:
            raise TypeError(f"{get_callable_name(f)}(): " + str(v_ex))
    return sig


def wrap_compat_transformer(name: str, f: AnyFun, sig: inspect.Signature, *args: Any, **kwargs: Any) -> AnyFun:
    """Creates a compatible wrapper over transformer function. A pure transformer function expects data item in first argument and one keyword argument called `meta`"""
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


def wrap_resource_gen(name: str, f: AnyFun, sig: inspect.Signature, *args: Any, **kwargs: Any) -> AnyFun:
    """Wraps a generator or generator function so it is evaluated on extraction"""
    if inspect.isgeneratorfunction(inspect.unwrap(f)) or inspect.isgenerator(f):
        # if no arguments then no wrap
        # if len(sig.parameters) == 0:
        #     return f

        # always wrap generators and generator functions. evaluate only at runtime!

        def _partial() -> Any:
            # print(f"_PARTIAL: {args} {kwargs} vs {args_}{kwargs_}")
            return f(*args, **kwargs)

        # this partial preserves the original signature and just defers the call to pipe
        return makefun.wraps(f, new_sig=inspect.signature(_partial))(_partial)  # type: ignore
    else:
        raise InvalidResourceDataTypeFunctionNotAGenerator(name, f, type(f))
