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
    """A decorator that transforms a function returning one or more `dlt resources` into a `dlt source` in order to load it with `dlt`.

    ### Summary
    A `dlt source` is a logical grouping of resources that are often extracted and loaded together. A source is associated with a schema, which describes the structure of the loaded data and provides instructions how to load it.
    Such schema contains table schemas that describe the structure of the data coming from the resources. See https://dlthub.com/docs/glossary for more basic term definitions.

    ### Passing credentials
    Another important function of the source decorator is to provide credentials and other configuration to the code that extracts data. The decorator may automatically bind the source function arguments to the secret and config values.
    >>> def chess(username, chess_url: str = dlt.config.value, api_secret = dlt.secret.value, title: str = "GM"):
    >>>     return user_profile(username, chess_url, api_secret), user_games(username, chess_url, api_secret, with_titles=title)
    >>>
    >>> list(chess("magnuscarlsen"))

    Here `username` is a required, explicit python argument, `chess_url` is a required argument, that if not explicitly passed will be taken from configuration ie. `config.toml`, `api_secret` is a required argument, that if not explicitly passed will be taken from dlt secrets ie. `secrets.toml`.
    See https://dlthub.com/docs/customization/credentials for details.

    ### Args:
        func: A function that returns a dlt resource or a list of those or a list of any data items that can be loaded by `dlt`.

        name (str, optional): A name of the source which is also the name of the associated schema. If not present, the function name will be used.

        max_table_nesting (int, optional): A schema hint that sets the maximum depth of nested table beyond which the remaining nodes are loaded as string.

        schema (Schema, optional): An explicit `Schema` instance to be associated with the source. If not present, `dlt` creates a new `Schema` object with provided `name`. If such `Schema` already exists in the same folder as the module containing the decorated function, such schema will be loaded from file.

        spec (Type[BaseConfiguration], optional): A specification of configuration and secret values required by the source.

    Returns:
        `DltSource` instance
    """

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
    """When used as a decorator, transforms any generator (yielding) function into a `dlt resource`. When used as a function, it transforms data in `data` argument into a `dlt resource`.

    ### Summary
    A `resource`is a location within a `source` that holds the data with specific structure (schema) or coming from specific origin. A resource may be a rest API endpoint, table in the database or a tab in Google Sheets.
    A `dlt resource` is python representation of a `resource` that combines both data and metadata (table schema) that describes the structure and instructs the loading of the data.
    A `dlt resource` is also an `Iterable` and can used like any other similar object ie. list or tuple. See https://dlthub.com/docs/glossary for more on basic term definitions.

    ### Passing credentials
    If used as a decorator (`data` argument is a `Generator`), it may automatically bind the source function arguments to the secret and config values.
    >>> def user_games(username, chess_url: str = dlt.config.value, api_secret = dlt.secret.value):
    >>>     return requests.get("%s/games/%s" % (chess_url, username), headers={"Authorization": f"Bearer {api_secret}"})
    >>>
    >>> list(user_games("magnuscarlsen"))

    Here `username` is a required, explicit python argument, `chess_url` is a required argument, that if not explicitly passed will be taken from configuration ie. `config.toml`, `api_secret` is a required argument, that if not explicitly passed will be taken from dlt secrets ie. `secrets.toml`.
    See https://dlthub.com/docs/customization/credentials for details.
    Note that if decorated function is an inner function, passing of the credentials will be disabled.

    ### Args:
        data (Callable | Any, optional): a function to be decorated or a data compatible with `dlt` `run`.

        name (str, optional): A name of the resource that by default also becomes the name of the table to which the data is loaded.
        If not present, the name of the decorated function will be used.

        table_name (TTableHintTemplate[str], optional): An table name, if different from `name`.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        write_disposition (Literal["skip", "append", "replace"], optional): Controls how to write data to a table. `append` will always add new data at the end of the table. `replace` will replace existing data with new data. `skip` will prevent data from loading. . Defaults to "append".
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        columns (Sequence[TColumnSchema], optional): A list of column schemas. Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        selected (bool, optional): When `True` `dlt pipeline` will extract and load this resource, if `False`, the resource will be ignored.

        spec (Type[BaseConfiguration], optional): A specification of configuration and secret values required by the source.

        depends_on (TUnboundDltResource, optional): Allows to pipe data from one resource to another to build multi-step pipelines.

    ### Raises
        ResourceNameMissing: indicates that name of the resource cannot be inferred from the `data` being passed.
        InvalidResourceDataType: indicates that the `data` argument cannot be converted into `dlt resource`

    Returns:
        DltResource instance which may be loaded, iterated or combined with other resources into a pipeline.
    """
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
    """A form of `dlt resource` that takes input from other resources in order to enrich or transformer the data.

    ### Example
    >>> @dlt.resource
    >>> def players(title, chess_url=dlt.config.value):
    >>>     r = requests.get(f"{chess_url}titled/{title}")
    >>>     yield r.json()["players"]  # returns list of player names
    >>>
    >>> # this resource takes data from players and returns profiles
    >>> @dlt.transformer(data_from=players, write_disposition="replace")
    >>> def player_profile(player: Any) -> Iterator[TDataItems]:
    >>>     r = requests.get(f"{chess_url}player/{player}")
    >>>     r.raise_for_status()
    >>>     yield r.json()
    >>>
    >>> list(players("GM") | player_profile)  # pipes the data from players into player profile to produce a list of player profiles

    """
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
