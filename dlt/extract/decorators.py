import os
import inspect
from types import ModuleType
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict, Iterator, List, NamedTuple, Optional, Tuple, Type, TypeVar, Union, cast, overload

from dlt.common.configuration import with_config, get_fun_spec, known_sections, configspec
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ContextDefaultCannotBeCreated
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import BaseConfiguration, ContainerInjectableContext
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.exceptions import ArgumentsOverloadException
from dlt.common.pipeline import PipelineContext
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TColumnKey, TColumnName, TTableSchemaColumns, TWriteDisposition
from dlt.common.storages.exceptions import SchemaNotFoundError
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.typing import AnyFun, ParamSpec, Concatenate, TDataItem, TDataItems
from dlt.common.utils import get_callable_name, get_module_name, is_inner_callable
from dlt.extract.exceptions import InvalidTransformerDataTypeGeneratorFunctionRequired, ResourceFunctionExpected, ResourceInnerCallableConfigWrapDisallowed, SourceDataIsNone, SourceIsAClassTypeError, SourceNotAFunction, SourceSchemaNotAvailable
from dlt.extract.incremental import IncrementalResourceWrapper

from dlt.extract.typing import TTableHintTemplate
from dlt.extract.source import DltResource, DltSource, TUnboundDltResource



@configspec(init=True)
class SourceSchemaInjectableContext(ContainerInjectableContext):
    """A context containing the source schema, present when decorated function is executed"""
    schema: Schema

    can_create_default: ClassVar[bool] = False

    if TYPE_CHECKING:
        def __init__(self, schema: Schema = None) -> None:
            ...


class SourceInfo(NamedTuple):
    """Runtime information on the source/resource"""
    SPEC: Type[BaseConfiguration]
    f: AnyFun
    module: ModuleType


_SOURCES: Dict[str, SourceInfo] = {}
"""A registry of all the decorated sources and resources discovered when importing modules"""

TSourceFunParams = ParamSpec("TSourceFunParams")
TResourceFunParams = ParamSpec("TResourceFunParams")


@overload
def source(
    func: Callable[TSourceFunParams, Any],
    /,
    name: str = None,
    section: str = None,
    max_table_nesting: int = None,
    root_key: bool = False,
    schema: Schema = None,
    spec: Type[BaseConfiguration] = None
) -> Callable[TSourceFunParams, DltSource]:
    ...

@overload
def source(
    func: None = ...,
    /,
    name: str = None,
    section: str = None,
    max_table_nesting: int = None,
    root_key: bool = False,
    schema: Schema = None,
    spec: Type[BaseConfiguration] = None
) -> Callable[[Callable[TSourceFunParams, Any]], Callable[TSourceFunParams, DltSource]]:
    ...

def source(
    func: Optional[AnyFun] = None,
    /,
    name: str = None,
    section: str = None,
    max_table_nesting: int = None,
    root_key: bool = False,
    schema: Schema = None,
    spec: Type[BaseConfiguration] = None
) -> Any:
    """A decorator that transforms a function returning one or more `dlt resources` into a `dlt source` in order to load it with `dlt`.

    ### Summary
    A `dlt source` is a logical grouping of resources that are often extracted and loaded together. A source is associated with a schema, which describes the structure of the loaded data and provides instructions how to load it.
    Such schema contains table schemas that describe the structure of the data coming from the resources.

    Please refer to https://dlthub.com/docs/general-usage/source for a complete documentation.

    ### Passing credentials
    Another important function of the source decorator is to provide credentials and other configuration to the code that extracts data. The decorator may automatically bind the source function arguments to the secret and config values.
    >>> @dlt.source
    >>> def chess(username, chess_url: str = dlt.config.value, api_secret = dlt.secrets.value, title: str = "GM"):
    >>>     return user_profile(username, chess_url, api_secret), user_games(username, chess_url, api_secret, with_titles=title)
    >>>
    >>> list(chess("magnuscarlsen"))

    Here `username` is a required, explicit python argument, `chess_url` is a required argument, that if not explicitly passed will be taken from configuration ie. `config.toml`, `api_secret` is a required argument, that if not explicitly passed will be taken from dlt secrets ie. `secrets.toml`.
    See https://dlthub.com/docs/general-usage/credentials for details.

    ### Args:
        func: A function that returns a dlt resource or a list of those or a list of any data items that can be loaded by `dlt`.

        name (str, optional): A name of the source which is also the name of the associated schema. If not present, the function name will be used.

        section (str, optional): A name of configuration and state sections. If not present, the current python module name will be used.

        max_table_nesting (int, optional): A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.

        root_key (bool): Enables merging on all resources by propagating root foreign key to child tables. This option is most useful if you plan to change write disposition of a resource to disable/enable merge. Defaults to False.

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

        # wrap source extraction function in configuration with section
        func_module = inspect.getmodule(f)
        source_section = section or _get_source_section_name(func_module)
        source_sections = (known_sections.SOURCES, source_section, name)
        conf_f = with_config(f, spec=spec, sections=source_sections)

        @wraps(conf_f)
        def _wrap(*args: Any, **kwargs: Any) -> DltSource:
            # make schema available to the source
            with Container().injectable_context(SourceSchemaInjectableContext(schema)):
                # configurations will be accessed in this section in the source
                proxy = Container()[PipelineContext]
                pipeline_name = None if not proxy.is_active() else proxy.pipeline().pipeline_name
                with inject_section(ConfigSectionContext(pipeline_name=pipeline_name, sections=source_sections)):
                    rv = conf_f(*args, **kwargs)

            if rv is None:
                raise SourceDataIsNone(name)

            # if generator, consume it immediately
            if inspect.isgenerator(rv):
                rv = list(rv)

            # convert to source
            s = DltSource.from_data(name, source_section, schema.clone(), rv)
            # apply hints
            if max_table_nesting is not None:
                s.max_table_nesting = max_table_nesting
            # enable root propagation
            s.root_key = root_key
            return s


        # get spec for wrapped function
        SPEC = get_fun_spec(conf_f)
        # store the source information
        _SOURCES[_wrap.__qualname__] = SourceInfo(SPEC, _wrap, func_module)

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
    primary_key: TTableHintTemplate[TColumnKey] = None,
    merge_key: TTableHintTemplate[TColumnKey] = None,
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
    primary_key: TTableHintTemplate[TColumnKey] = None,
    merge_key: TTableHintTemplate[TColumnKey] = None,
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
    primary_key: TTableHintTemplate[TColumnKey] = None,
    merge_key: TTableHintTemplate[TColumnKey] = None,
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
    primary_key: TTableHintTemplate[TColumnKey] = None,
    merge_key: TTableHintTemplate[TColumnKey] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    depends_on: TUnboundDltResource = None,
) -> Any:
    """When used as a decorator, transforms any generator (yielding) function into a `dlt resource`. When used as a function, it transforms data in `data` argument into a `dlt resource`.

    ### Summary
    A `resource`is a location within a `source` that holds the data with specific structure (schema) or coming from specific origin. A resource may be a rest API endpoint, table in the database or a tab in Google Sheets.
    A `dlt resource` is python representation of a `resource` that combines both data and metadata (table schema) that describes the structure and instructs the loading of the data.
    A `dlt resource` is also an `Iterable` and can used like any other iterable object ie. list or tuple.

    Please refer to https://dlthub.com/docs/general-usage/resource for a complete documentation.

    ### Passing credentials
    If used as a decorator (`data` argument is a `Generator`), it may automatically bind the source function arguments to the secret and config values.
    >>> @dlt.resource
    >>> def user_games(username, chess_url: str = dlt.config.value, api_secret = dlt.secrets.value):
    >>>     return requests.get("%s/games/%s" % (chess_url, username), headers={"Authorization": f"Bearer {api_secret}"})
    >>>
    >>> list(user_games("magnuscarlsen"))

    Here `username` is a required, explicit python argument, `chess_url` is a required argument, that if not explicitly passed will be taken from configuration ie. `config.toml`, `api_secret` is a required argument, that if not explicitly passed will be taken from dlt secrets ie. `secrets.toml`.
    See https://dlthub.com/docs/general-usage/credentials for details.
    Note that if decorated function is an inner function, passing of the credentials will be disabled.

    ### Args:
        data (Callable | Any, optional): a function to be decorated or a data compatible with `dlt` `run`.

        name (str, optional): A name of the resource that by default also becomes the name of the table to which the data is loaded.
        If not present, the name of the decorated function will be used.

        table_name (TTableHintTemplate[str], optional): An table name, if different from `name`.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        write_disposition (Literal["skip", "append", "replace", "merge"], optional): Controls how to write data to a table. `append` will always add new data at the end of the table. `replace` will replace existing data with new data. `skip` will prevent data from loading. "merge" will deduplicate and merge data based on "primary_key" and "merge_key" hints. Defaults to "append".
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        columns (Sequence[TColumnSchema], optional): A list of column schemas. Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        primary_key (str | Sequence[str]): A column name or a list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        merge_key (str | Sequence[str]): A column name or a list of column names that define a merge key. Typically used with "merge" write disposition to remove overlapping data ranges ie. to keep a single record for a given day.
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
    def make_resource(_name: str, _section: str, _data: Any, incremental: IncrementalResourceWrapper = None) -> DltResource:
        table_template = DltResource.new_table_template(
            table_name or _name,
            write_disposition=write_disposition,
            columns=columns,
            primary_key=primary_key,
            merge_key=merge_key
        )
        return DltResource.from_data(_data, _name, _section, table_template, selected, cast(DltResource, depends_on), incremental=incremental)


    def decorator(f: Callable[TResourceFunParams, Any]) -> Callable[TResourceFunParams, DltResource]:
        if not callable(f):
            if depends_on:
                # raise more descriptive exception if we construct transformer
                raise InvalidTransformerDataTypeGeneratorFunctionRequired(name or "<no name>", f, type(f))
            raise ResourceFunctionExpected(name or "<no name>", f, type(f))

        resource_name = name or get_callable_name(f)

        # do not inject config values for inner functions, we assume that they are part of the source
        SPEC: Type[BaseConfiguration] = None
        # wrap source extraction function in configuration with section
        func_module = inspect.getmodule(f)
        source_section = _get_source_section_name(func_module)

        incremental: IncrementalResourceWrapper = None
        sig = inspect.signature(f)
        if IncrementalResourceWrapper.should_wrap(sig):
            incremental = IncrementalResourceWrapper(resource_name, primary_key)
        incr_f = incremental.wrap(sig, f) if incremental else f

        resource_sections = (known_sections.SOURCES, source_section, resource_name)
        # standalone resource will prefer existing section context when resolving config values
        # this lets the source to override those values and provide common section for all config values for resources present in that source
        conf_f = with_config(
            incr_f,
            spec=spec, sections=resource_sections, sections_merge_style=ConfigSectionContext.resource_merge_style, include_defaults=False
        )
        is_inner_resource = is_inner_callable(f)
        if conf_f != incr_f and is_inner_resource:
            raise ResourceInnerCallableConfigWrapDisallowed(resource_name, source_section)
        # get spec for wrapped function
        SPEC = get_fun_spec(conf_f)

        # store the standalone resource information
        if not is_inner_resource:
            _SOURCES[f.__qualname__] = SourceInfo(SPEC, f, func_module)

        return make_resource(resource_name, source_section, conf_f, incremental)

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
        return make_resource(name, None, data)


def transformer(
    data_from: TUnboundDltResource = DltResource.Empty,
    name: str = None,
    table_name: TTableHintTemplate[str] = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TTableSchemaColumns] = None,
    primary_key: TTableHintTemplate[TColumnKey] = None,
    merge_key: TTableHintTemplate[TColumnKey] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None
) -> Callable[[Callable[Concatenate[TDataItem, TResourceFunParams], Any]], Callable[TResourceFunParams, DltResource]]:
    """A form of `dlt resource` that takes input from other resources in order to enrich or transform the data.

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
    return resource(  # type: ignore
        f,
        name=name,
        table_name=table_name,
        write_disposition=write_disposition,
        columns=columns,
        primary_key=primary_key,
        merge_key=merge_key,
        selected=selected,
        spec=spec,
        depends_on=data_from
    )


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


def _get_source_section_name(m: ModuleType) -> str:
    if hasattr(m, "__source_name__"):
        return cast(str, m.__source_name__)
    return get_module_name(m)


def get_source_schema() -> Schema:
    """When executed from the function decorated with @dlt.source, returns a writable source Schema"""
    try:
        return Container()[SourceSchemaInjectableContext].schema
    except ContextDefaultCannotBeCreated:
        raise SourceSchemaNotAvailable()


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
