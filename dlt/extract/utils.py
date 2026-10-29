import inspect
from typing import (
    TYPE_CHECKING,
    Callable,
    Collection,
    Dict,
    Mapping,
    Optional,
    Tuple,
    Union,
    List,
    Any,
    Sequence,
    cast,
    AsyncIterator,
    Awaitable,
    Generator,
    Iterable,
    Iterator,
)
from collections.abc import Mapping as C_Mapping
from functools import wraps

import humanize

from dlt.common.data_writers import TDataItemFormat
from dlt.common.json import json
from dlt.common.metrics import DataWriterMetrics
from dlt.common.storages.job_metrics import metrics_by_job
from dlt.common.storages.normalize_storage import group_worker_files
from dlt.common.time import datetime_to_timestamp_us
from dlt.common.libs import (
    get_pandas_module,
    get_polars_module,
    get_pyarrow_module,
    get_pydantic_module,
    is_arrow_object,
    is_arrow_schema,
    is_pandas_frame,
    is_polars_frame,
)
from dlt.common.reflection.inspect import isgeneratorfunction
from dlt.common.schema.typing import TAnySchemaColumns, TTableSchemaColumns
from dlt.common.schema.utils import normalize_schema_name
from dlt.common.typing import (
    Self,
    AnyFun,
    TDataItem,
    TDataItems,
    TAnyFunOrGenerator,
    TColumnNames,
    NoneType,
)
from dlt.common.utils import (
    get_callable_name,
    get_full_callable_name,
    get_main_module_name,
    digest128,
)

from dlt.extract.exceptions import (
    InvalidResourceDataTypeIsNone,
    InvalidResourceReturnsResource,
    InvalidStepFunctionArguments,
)
from dlt.extract.items import TTableHintTemplate

if TYPE_CHECKING:
    from dlt.common.libs.pyarrow import pyarrow as pa

NORMALIZE_PARALLEL_MIN_BYTES = 32 * 1024 * 1024
"""Data to normalize below this size takes seconds, so idle normalize workers are not reported"""
NORMALIZE_PARALLEL_MIN_USAGE = 0.5
"""Files are reported as unbalanced when normalize would use at most this share of its workers"""


def get_data_item_format(items: TDataItems) -> TDataItemFormat:
    """Detect the format of the data item from `items`.

    Reverts to `object` for empty lists

    Returns:
        The data file format.
    """

    # if incoming item is hints meta, check if item format is forced
    from dlt.dataset.relation import Relation

    if isinstance(items, Relation):
        return "model"

    if get_pyarrow_module() is None and get_pandas_module() is None and get_polars_module() is None:
        return "object"

    # Assume all items in list are the same type
    try:
        if isinstance(items, list):
            items = items[0]
        if is_arrow_object(items) or is_pandas_frame(items) or is_polars_frame(items):
            return "arrow"
    except IndexError:
        pass
    return "object"


def get_arrow_fields(items: TDataItems) -> Dict[str, "pa.Field"]:
    """Returns arrow fields of `items` by name, later items override earlier ones."""
    from dlt.common.libs.pyarrow import pyarrow

    fields: Dict[str, "pa.Field"] = {}
    for item in items if isinstance(items, list) else [items]:
        for field in item.schema:
            # null fields do not create columns
            if not pyarrow.types.is_null(field.type):
                fields[field.name] = field
    return fields


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


DEDUP_HASH_BYTES = 9
"""Digest length of an incremental dedup hash, 12 characters in base64."""
LEGACY_DEDUP_HASH_LEN = 20
"""Length of a dedup hash written before 1.29: a 15 byte digest over the `Z` rendering of UTC."""


def digest_dedup_value(value: Any, legacy: bool = False) -> str:
    """Stable content hash of a JSON-serializable value used for incremental dedup.

    `legacy` reproduces the hash written before 1.29, so hashes kept in older state still match.
    """
    if legacy:
        return digest128(json.dumps(value, sort_keys=True, utc_z=True))
    # rows and SQL aggregates carry one instant in different zones or naive, all must hash alike
    return digest128(
        json.dumps(value, sort_keys=True, datetime_encoder=datetime_to_timestamp_us),
        DEDUP_HASH_BYTES,
    )


def has_legacy_dedup_hashes(hashes: Iterable[str]) -> bool:
    """True when `hashes` were written before 1.29 and only match `legacy` digests."""
    return any(len(h) == LEGACY_DEDUP_HASH_LEN for h in hashes)


def ensure_table_schema_columns(columns: TAnySchemaColumns) -> TTableSchemaColumns:
    """Convert supported column schema types to a column dict which
    can be used in resource schema.

    Args:
        columns: A dict of column schemas, a list of column schemas, a pydantic model, or a pyarrow schema
    """
    if is_arrow_schema(columns):
        from dlt.common.libs.pyarrow import pyarrow, py_arrow_to_table_schema_columns

        return py_arrow_to_table_schema_columns(cast(pyarrow.Schema, columns))
    if isinstance(columns, C_Mapping):
        # fill missing names in short form was used
        for col_name in columns:
            columns[col_name]["name"] = col_name
        return columns
    elif isinstance(columns, Sequence):
        # Assume list of columns
        return {col["name"]: col for col in columns}
    elif get_pydantic_module() is not None:
        from dlt.common.libs import pydantic

        if isinstance(columns, pydantic.BaseModel) or issubclass(columns, pydantic.BaseModel):
            return pydantic.pydantic_to_table_schema_columns(columns)

    raise ValueError(f"Unsupported columns type: `{type(columns)}`")


def ensure_table_schema_columns_hint(
    columns: TTableHintTemplate[TAnySchemaColumns],
) -> TTableHintTemplate[TTableSchemaColumns]:
    """Convert column schema hint to a hint returning `TTableSchemaColumns`.
    A callable hint is wrapped in another function which converts the original result.
    """
    if callable(columns) and not isinstance(columns, type):

        def wrapper(item: TDataItem) -> TTableSchemaColumns:
            return ensure_table_schema_columns(columns(item))

        return wrapper

    return ensure_table_schema_columns(columns)


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


def wrap_iterator(gen: Iterator[TDataItems]) -> Iterator[TDataItems]:
    """Wraps an iterator into a generator"""
    if inspect.isgenerator(gen):
        return gen

    def wrapped_gen() -> Iterator[TDataItems]:
        yield from gen

    return wrapped_gen()


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
    # this gets called from the main thread when the wrapping generator is closed
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
        if isgeneratorfunction(f):
            return wraps(f)(_gen_wrapper)  # type: ignore[return-value]
        else:

            def _fun_wrapper(*args: Any, **kwargs: Any) -> Any:
                def _curry() -> Any:
                    return f(*args, **kwargs)

                return _curry

            return wraps(f)(_fun_wrapper)  # type: ignore[return-value]
    return _gen_wrapper()  # type: ignore[return-value]


def _transformer_compat(item: TDataItems, meta: Any = None) -> Any:
    pass


_transformer_compat_sig = inspect.signature(_transformer_compat)


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
    _wrapper = wraps(f)(_tx_partial)
    setattr(_wrapper, "__signature__", _transformer_compat_sig)
    return _wrapper


def _resource_compat() -> Any:
    pass


_resource_compat_sig = inspect.signature(_resource_compat)


def wrap_resource_gen(
    name: str, f: AnyFun, sig: inspect.Signature, *args: Any, **kwargs: Any
) -> AnyFun:
    """Wraps a generator or generator function so it is evaluated on extraction and binds argument passed to call"""

    def _partial() -> Any:
        # print(f"_PARTIAL: {args} {kwargs}")
        rv = f(*args, **kwargs)
        if rv is None:
            raise InvalidResourceDataTypeIsNone(name, rv, NoneType)
        # is it Pipe or resource
        if hasattr(rv, "_gen_idx") or hasattr(rv, "_pipe"):
            raise InvalidResourceReturnsResource(name, f, type(f))
        return rv

    _wrapper = wraps(f)(_partial)
    setattr(_wrapper, "__signature__", _resource_compat_sig)
    return _wrapper


def make_schema_with_default_name(pipeline_name: str) -> str:
    """Makes schema name from the pipeline name using the name normalizer. "_pipeline" suffix is removed if present"""
    if pipeline_name.endswith("_pipeline"):
        schema_name = pipeline_name[:-9]
    else:
        schema_name = pipeline_name
    return normalize_schema_name(schema_name)


def get_unbalanced_files_warning(
    pipeline_name: str,
    load_id: str,
    files: Sequence[DataWriterMetrics],
    file_limits: Mapping[str, Tuple[Optional[int], Optional[int]]],
    dlt_tables: Collection[str],
    workers: int,
) -> Optional[str]:
    """Returns a warning if `files` leave at least half of the normalize `workers` capacity idle.

    `file_limits` maps table names to `file_max_items` and `file_max_bytes` of their writers.
    """
    from dlt._workspace.cli.echo import cli_cmd

    if workers <= 1:
        return None
    sizes: Dict[str, int] = {}
    # file size stands for processing time. normalize links or copies all files except typed-jsonl
    costs: Dict[str, int] = {}
    tables: Dict[str, str] = {}
    for job, metrics in metrics_by_job(files).items():
        file_name = job.file_name()
        sizes[file_name] = metrics.file_size
        costs[file_name] = metrics.file_size if job.file_format == "typed-jsonl" else 0
        tables[file_name] = job.table_name
    total = sum(costs.values())
    if total < NORMALIZE_PARALLEL_MIN_BYTES:
        return None

    groups = group_worker_files(list(sizes), workers, sizes)
    loads = [sum(costs[file_name] for file_name in group) for group in groups]
    busiest = max(loads)
    # the busiest worker sets the run time, the others idle once they are done
    usage = total / (workers * busiest)
    if usage > NORMALIZE_PARALLEL_MIN_USAGE:
        return None

    message = (
        f"dlt extracted {len(costs)} {'file' if len(costs) == 1 else 'files'}"
        f" ({humanize.naturalsize(total, binary=True)}) into load package {load_id}."
        f" Normalize would use {usage:.0%} of {workers} workers, the busiest one gets"
        f" {humanize.naturalsize(busiest, binary=True)}."
    )
    # the table with most data on the busiest worker is the one to split
    busiest_costs: Dict[str, int] = {}
    for file_name in groups[loads.index(busiest)]:
        table_name = tables[file_name]
        busiest_costs[table_name] = busiest_costs.get(table_name, 0) + costs[file_name]
    table_name = max(busiest_costs, key=busiest_costs.__getitem__)
    if table_name not in dlt_tables:
        max_items, max_bytes = file_limits.get(table_name, (None, None))
        if max_items is None and max_bytes is None:
            remedy = "Set `file_max_items` or `file_max_bytes` in `[sources.data_writer]`"
        elif max_bytes is None:
            remedy = f"Lower `file_max_items` (now {max_items}) or set `file_max_bytes`"
        elif max_items is None:
            remedy = f"Lower `file_max_bytes` (now {max_bytes}) or set `file_max_items`"
        else:
            remedy = (
                f"Lower `file_max_items` (now {max_items}) or `file_max_bytes` (now {max_bytes})"
            )
        message += f" {remedy} to split the files of `{table_name}`."
    metrics_cmd = cli_cmd(f"pipeline {pipeline_name} trace metrics --steps extract -v")
    message += (
        f" Run `{metrics_cmd}` after the run to see the extracted files."
        " See https://dlthub.com/docs/reference/performance"
        "#controlling-intermediary-file-size-and-rotation"
    )
    return message


class dynstr(str):
    """Dynamic string which will generate final value when called"""

    __slots__ = "dyn"

    def __new__(cls, placeholder: str, dyn: Callable[..., str]) -> Self:
        # create a new str instance
        instance = super().__new__(cls, placeholder)
        return instance

    def __init__(self, placeholder: str, dyn: Callable[..., str]):
        super().__init__()
        self.dyn = dyn

    def __call__(self, param: Any) -> str:
        return self.dyn(param)


def get_code_ref(data: Any) -> Optional[str]:
    """Returns the fully qualified name of the function `data` is or was created from.

    Returns `None` for data that is not a function or a generator, and for anonymous code like
    lambdas or generator expressions, whose names are not unique within a module.
    """
    if inspect.isfunction(data):
        ref = get_full_callable_name(data)
    elif inspect.isgenerator(data) or inspect.isasyncgen(data):
        # a generator keeps its function name but not its module, take it from the frame
        frame = data.gi_frame if inspect.isgenerator(data) else data.ag_frame
        if frame is None:
            return None
        module = frame.f_globals.get("__name__")
        if not module:
            return None
        if module == "__main__":
            module = get_main_module_name()
        ref = module + "." + data.__qualname__
    else:
        return None
    # every `<lambda>` or `<genexpr>` in a module has the same name and would join unrelated code
    if any(part.startswith("<") and part != "<locals>" for part in ref.split(".")):
        return None
    return ref
