import os
import inspect
from types import ModuleType
from functools import wraps
from typing import Any, Callable, Dict, Iterator, List, NamedTuple, Optional, Tuple, Type, TypeVar, Union, cast, overload

from dlt.common.configuration import with_config, get_fun_spec
from dlt.common.configuration.resolve import inject_namespace
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.configuration.specs.config_namespace_context import ConfigNamespacesContext
from dlt.common.exceptions import ArgumentsOverloadException
from dlt.common.normalizers.json import relational as relational_normalizer
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TTableSchemaColumns, TWriteDisposition
from dlt.common.storages.exceptions import SchemaNotFoundError
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.typing import AnyFun, ParamSpec, Concatenate, TDataItem, TDataItems
from dlt.common.utils import get_callable_name, is_inner_callable
from dlt.extract.exceptions import InvalidTransformerDataTypeGeneratorFunctionRequired, ResourceFunctionExpected, SourceDataIsNone, SourceIsAClassTypeError, SourceNotAFunction

from dlt.extract.typing import TTableHintTemplate
from dlt.extract.source import DltResource, DltSource, TUnboundDltResource


class SourceInfo(NamedTuple):
    SPEC: Type[BaseConfiguration]
    f: AnyFun
    module: ModuleType


_SOURCES: Dict[str, SourceInfo] = {}

TSourceFunParams = ParamSpec("TSourceFunParams")
TResourceFunParams = ParamSpec("TResourceFunParams")


@overload
def source(func: Callable[TSourceFunParams, Any], /, name: str = None, max_table_nesting: int = None, schema: Schema = None, spec: Type[BaseConfiguration] = None) -> Callable[TSourceFunParams, DltSource]:
    ...

@overload
def source(func: None = ..., /, name: str = None, max_table_nesting: int = None, schema: Schema = None, spec: Type[BaseConfiguration] = None) -> Callable[[Callable[TSourceFunParams, Any]], Callable[TSourceFunParams, DltSource]]:
    ...

def source(func: Optional[AnyFun] = None, /, name: str = None, max_table_nesting: int = None, schema: Schema = None, spec: Type[BaseConfiguration] = None) -> Any:

    if name and schema:
        raise ArgumentsOverloadException("'name' has no effect when `schema` argument is present", source.__name__)

    def decorator(f: Callable[TSourceFunParams, Any]) -> Callable[TSourceFunParams, DltSource]:
        nonlocal schema, name

        if not callable(f) or isinstance(f, DltResource):
            raise SourceNotAFunction(name or "<no name>", f, type(f))

        if inspect.isclass(f):
            raise SourceIsAClassTypeError(name or "<no name>", f)

        # source name is passed directly or taken from decorated function name
        name = name or get_callable_name(f)

        if not schema:
            # load the schema from file with name_schema.yaml/json from the same directory, the callable resides OR create new default schema
            schema = _maybe_load_schema_for_callable(f, name) or Schema(name, normalize_name=True)

        if max_table_nesting is not None:
            # limit the number of levels in the parent-child table hierarchy
            relational_normalizer.update_normalizer_config(schema,
                {
                    "max_nesting": max_table_nesting
                }
            )

        # wrap source extraction function in configuration with namespace
        source_namespaces = ("sources", name)
        conf_f = with_config(f, spec=spec, namespaces=source_namespaces)

        @wraps(conf_f)
        def _wrap(*args: Any, **kwargs: Any) -> DltSource:
            # configurations will be accessed in this namespace in the source
            with inject_namespace(ConfigNamespacesContext(namespaces=source_namespaces)):
                rv = conf_f(*args, **kwargs)

            # if generator, consume it immediately
            if inspect.isgenerator(rv):
                rv = list(rv)

            if rv is None:
                raise SourceDataIsNone(name)

            # convert to source
            return DltSource.from_data(name, schema, rv)

        # get spec for wrapped function
        SPEC = get_fun_spec(conf_f)
        # store the source information
        _SOURCES[_wrap.__qualname__] = SourceInfo(SPEC, _wrap, inspect.getmodule(f))

        # the typing is right, but makefun.wraps does not preserve signatures
        return _wrap

    if func is None:
        # we're called with parens.
        return decorator

    # we're called as @source without parens.
    return decorator(func)


@overload
def resource(
    data: Callable[TResourceFunParams, Any],
    /,
    name: str = None,
    table_name: TTableHintTemplate[str] = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TTableSchemaColumns] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None
) -> Callable[TResourceFunParams, DltResource]:
    ...

@overload
def resource(
    data: None = ...,
    /,
    name: str = None,
    table_name: TTableHintTemplate[str] = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TTableSchemaColumns] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None
) -> Callable[[Callable[TResourceFunParams, Any]], Callable[TResourceFunParams, DltResource]]:
    ...



@overload
def resource(
    data: Union[List[Any], Tuple[Any], Iterator[Any]],
    /,
    name: str = None,
    table_name: TTableHintTemplate[str] = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TTableSchemaColumns] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None
) -> DltResource:
    ...


def resource(
    data: Optional[Any] = None,
    /,
    name: str = None,
    table_name: TTableHintTemplate[str] = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TTableSchemaColumns] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    depends_on: TUnboundDltResource = None
) -> Any:

    def make_resource(_name: str, _data: Any) -> DltResource:
        table_template = DltResource.new_table_template(table_name or _name, write_disposition=write_disposition, columns=columns)
        return DltResource.from_data(_data, _name, table_template, selected, cast(DltResource, depends_on))


    def decorator(f: Callable[TResourceFunParams, Any]) -> Callable[TResourceFunParams, DltResource]:
        if not callable(f):
            if depends_on:
                # raise more descriptive exception if we construct transformer
                raise InvalidTransformerDataTypeGeneratorFunctionRequired(name or "<no name>", f, type(f))
            raise ResourceFunctionExpected(name or "<no name>", f, type(f))

        resource_name = name or get_callable_name(f)

        # do not inject config values for inner functions, we assume that they are part of the source
        SPEC: Type[BaseConfiguration] = None
        if is_inner_callable(f):
            conf_f = f
        else:
            # wrap source extraction function in configuration with namespace
            conf_f = with_config(f, spec=spec, namespaces=("sources", resource_name))
            # get spec for wrapped function
            SPEC = get_fun_spec(conf_f)

        # store the standalone resource information
        if SPEC:
            _SOURCES[f.__qualname__] = SourceInfo(SPEC, f, inspect.getmodule(f))

        return make_resource(resource_name, conf_f)

    # if data is callable or none use decorator
    if data is None:
        # we're called with parens.
        return decorator

    if callable(data):
        return decorator(data)
    else:
        # take name from the generator
        if inspect.isgenerator(data):
            name = name or get_callable_name(data)  # type: ignore
        return make_resource(name, data)


def transformer(
    data_from: TUnboundDltResource = DltResource.Empty,
    name: str = None,
    table_name: TTableHintTemplate[str] = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TTableSchemaColumns] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None
) -> Callable[[Callable[Concatenate[TDataItem, TResourceFunParams], Any]], Callable[TResourceFunParams, DltResource]]:
    f: AnyFun = None
    # if data_from is a function we are called without parens
    if inspect.isfunction(data_from):
        f = data_from
        data_from = DltResource.Empty
    return resource(f, name=name, table_name=table_name, write_disposition=write_disposition, columns=columns, selected=selected, spec=spec, depends_on=data_from)  # type: ignore


def _maybe_load_schema_for_callable(f: AnyFun, name: str) -> Optional[Schema]:
    if not inspect.isfunction(f):
        f = f.__class__
    try:
        file = inspect.getsourcefile(f)
        if file:
            return SchemaStorage.load_schema_file(os.path.dirname(file), name)
    except SchemaNotFoundError:
        pass
    return None


def _get_source_for_inner_function(f: AnyFun) -> Optional[SourceInfo]:
    # find source function
    parts = get_callable_name(f, "__qualname__").split(".")
    parent_fun = ".".join(parts[:-2])
    return _SOURCES.get(parent_fun)


# def with_retry(max_retries: int = 3, retry_sleep: float = 1.0) -> Callable[[Callable[_TFunParams, TBoundItem]], Callable[_TFunParams, TBoundItem]]:

#     def decorator(f: Callable[_TFunParams, TBoundItem]) -> Callable[_TFunParams, TBoundItem]:

#         def _wrap(*args: Any, **kwargs: Any) -> TBoundItem:
#             attempts = 0
#             while True:
#                 try:
#                     return f(*args, **kwargs)
#                 except Exception as exc:
#                     if attempts == max_retries:
#                         raise
#                     attempts += 1
#                     logger.warning(f"Exception {exc} in iterator, retrying {attempts} / {max_retries}")
#                     sleep(retry_sleep)

#         return _wrap

#     return decorator


TBoundItems = TypeVar("TBoundItems", bound=TDataItems)
TDeferred = Callable[[], TBoundItems]
TDeferredFunParams = ParamSpec("TDeferredFunParams")


def defer(f: Callable[TDeferredFunParams, TBoundItems]) -> Callable[TDeferredFunParams, TDeferred[TBoundItems]]:

    @wraps(f)
    def _wrap(*args: Any, **kwargs: Any) -> TDeferred[TBoundItems]:
        def _curry() -> TBoundItems:
            return f(*args, **kwargs)
        return _curry

    return _wrap
