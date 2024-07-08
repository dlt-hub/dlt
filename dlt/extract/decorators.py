import os
import inspect
from types import ModuleType
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)
from typing_extensions import TypeVar

from dlt.common.configuration import with_config, get_fun_spec, known_sections, configspec
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ContextDefaultCannotBeCreated
from dlt.common.configuration.inject import set_fun_spec
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import BaseConfiguration, ContainerInjectableContext
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.exceptions import ArgumentsOverloadException
from dlt.common.pipeline import PipelineContext
from dlt.common.reflection.spec import spec_from_signature
from dlt.common.schema.utils import DEFAULT_WRITE_DISPOSITION
from dlt.common.source import _SOURCES, SourceInfo
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import (
    TColumnNames,
    TFileFormat,
    TWriteDisposition,
    TWriteDispositionConfig,
    TAnySchemaColumns,
    TSchemaContract,
    TTableFormat,
)
from dlt.common.storages.exceptions import SchemaNotFoundError
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.typing import AnyFun, ParamSpec, Concatenate, TDataItem, TDataItems
from dlt.common.utils import get_callable_name, get_module_name, is_inner_callable

from dlt.extract.hints import make_hints
from dlt.extract.utils import simulate_func_call
from dlt.extract.exceptions import (
    CurrentSourceNotAvailable,
    DynamicNameNotStandaloneResource,
    InvalidResourceDataTypeFunctionNotAGenerator,
    InvalidTransformerDataTypeGeneratorFunctionRequired,
    ResourceFunctionExpected,
    ResourceInnerCallableConfigWrapDisallowed,
    SourceDataIsNone,
    SourceIsAClassTypeError,
    ExplicitSourceNameInvalid,
    SourceNotAFunction,
    CurrentSourceSchemaNotAvailable,
)
from dlt.extract.items import TTableHintTemplate
from dlt.extract.source import DltSource
from dlt.extract.resource import DltResource, TUnboundDltResource, TDltResourceImpl


@configspec
class SourceSchemaInjectableContext(ContainerInjectableContext):
    """A context containing the source schema, present when dlt.source/resource decorated function is executed"""

    schema: Schema = None

    can_create_default: ClassVar[bool] = False


@configspec
class SourceInjectableContext(ContainerInjectableContext):
    """A context containing the source schema, present when dlt.resource decorated function is executed"""

    source: DltSource = None

    can_create_default: ClassVar[bool] = False


TSourceFunParams = ParamSpec("TSourceFunParams")
TResourceFunParams = ParamSpec("TResourceFunParams")
TDltSourceImpl = TypeVar("TDltSourceImpl", bound=DltSource, default=DltSource)


@overload
def source(
    func: Callable[TSourceFunParams, Any],
    /,
    name: str = None,
    section: str = None,
    max_table_nesting: int = None,
    root_key: bool = False,
    schema: Schema = None,
    schema_contract: TSchemaContract = None,
    spec: Type[BaseConfiguration] = None,
    _impl_cls: Type[TDltSourceImpl] = DltSource,  # type: ignore[assignment]
) -> Callable[TSourceFunParams, TDltSourceImpl]: ...


@overload
def source(
    func: None = ...,
    /,
    name: str = None,
    section: str = None,
    max_table_nesting: int = None,
    root_key: bool = False,
    schema: Schema = None,
    schema_contract: TSchemaContract = None,
    spec: Type[BaseConfiguration] = None,
    _impl_cls: Type[TDltSourceImpl] = DltSource,  # type: ignore[assignment]
) -> Callable[[Callable[TSourceFunParams, Any]], Callable[TSourceFunParams, TDltSourceImpl]]: ...


def source(
    func: Optional[AnyFun] = None,
    /,
    name: str = None,
    section: str = None,
    max_table_nesting: int = None,
    root_key: bool = False,
    schema: Schema = None,
    schema_contract: TSchemaContract = None,
    spec: Type[BaseConfiguration] = None,
    _impl_cls: Type[TDltSourceImpl] = DltSource,  # type: ignore[assignment]
) -> Any:
    """A decorator that transforms a function returning one or more `dlt resources` into a `dlt source` in order to load it with `dlt`.

    #### Note:
    A `dlt source` is a logical grouping of resources that are often extracted and loaded together. A source is associated with a schema, which describes the structure of the loaded data and provides instructions how to load it.
    Such schema contains table schemas that describe the structure of the data coming from the resources.

    Please refer to https://dlthub.com/docs/general-usage/source for a complete documentation.

    #### Credentials:
    Another important function of the source decorator is to provide credentials and other configuration to the code that extracts data. The decorator may automatically bind the source function arguments to the secret and config values.
    >>> @dlt.source
    >>> def chess(username, chess_url: str = dlt.config.value, api_secret = dlt.secrets.value, title: str = "GM"):
    >>>     return user_profile(username, chess_url, api_secret), user_games(username, chess_url, api_secret, with_titles=title)
    >>>
    >>> list(chess("magnuscarlsen"))

    Here `username` is a required, explicit python argument, `chess_url` is a required argument, that if not explicitly passed will be taken from configuration ie. `config.toml`, `api_secret` is a required argument, that if not explicitly passed will be taken from dlt secrets ie. `secrets.toml`.
    See https://dlthub.com/docs/general-usage/credentials for details.

    #### Args:
        func: A function that returns a dlt resource or a list of those or a list of any data items that can be loaded by `dlt`.

        name (str, optional): A name of the source which is also the name of the associated schema. If not present, the function name will be used.

        section (str, optional): A name of configuration. If not present, the current python module name will be used.

        max_table_nesting (int, optional): A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.

        root_key (bool): Enables merging on all resources by propagating root foreign key to child tables. This option is most useful if you plan to change write disposition of a resource to disable/enable merge. Defaults to False.

        schema (Schema, optional): An explicit `Schema` instance to be associated with the source. If not present, `dlt` creates a new `Schema` object with provided `name`. If such `Schema` already exists in the same folder as the module containing the decorated function, such schema will be loaded from file.

        schema_contract (TSchemaContract, optional): Schema contract settings that will be applied to this resource.

        spec (Type[BaseConfiguration], optional): A specification of configuration and secret values required by the source.

        _impl_cls (Type[TDltSourceImpl], optional): A custom implementation of DltSource, may be also used to providing just a typing stub

    Returns:
        `DltSource` instance
    """
    if name and schema:
        raise ArgumentsOverloadException(
            "'name' has no effect when `schema` argument is present", source.__name__
        )

    def decorator(
        f: Callable[TSourceFunParams, Any]
    ) -> Callable[TSourceFunParams, Union[Awaitable[TDltSourceImpl], TDltSourceImpl]]:
        nonlocal schema, name

        if not callable(f) or isinstance(f, DltResource):
            raise SourceNotAFunction(name or "<no name>", f, type(f))

        if inspect.isclass(f):
            raise SourceIsAClassTypeError(name or "<no name>", f)

        # source name is passed directly or taken from decorated function name
        effective_name = name or get_callable_name(f)

        if schema and name and name != schema.name:
            raise ExplicitSourceNameInvalid(name, schema.name)

        # wrap source extraction function in configuration with section
        func_module = inspect.getmodule(f)
        source_section = section or _get_source_section_name(func_module)
        # use effective_name which is explicit source name or callable name to represent third element in source config path
        source_sections = (known_sections.SOURCES, source_section, effective_name)
        conf_f = with_config(f, spec=spec, sections=source_sections)

        def _eval_rv(_rv: Any, schema_copy: Schema) -> TDltSourceImpl:
            """Evaluates return value from the source function or coroutine"""
            if _rv is None:
                raise SourceDataIsNone(schema_copy.name)
            # if generator, consume it immediately
            if inspect.isgenerator(_rv):
                _rv = list(_rv)

            # convert to source
            s = _impl_cls.from_data(schema_copy, source_section, _rv)
            # apply hints
            if max_table_nesting is not None:
                s.max_table_nesting = max_table_nesting
            s.schema_contract = schema_contract
            # enable root propagation
            s.root_key = root_key
            return s

        def _make_schema() -> Schema:
            if not schema:
                # load the schema from file with name_schema.yaml/json from the same directory, the callable resides OR create new default schema
                return _maybe_load_schema_for_callable(f, effective_name) or Schema(effective_name)
            else:
                # clone the schema passed to decorator, update normalizers, remove processing hints
                # NOTE: source may be called several times in many different settings
                return schema.clone(update_normalizers=True, remove_processing_hints=True)

        @wraps(conf_f)
        def _wrap(*args: Any, **kwargs: Any) -> TDltSourceImpl:
            """Wrap a regular function, injection context must be a part of the wrap"""
            schema_copy = _make_schema()
            with Container().injectable_context(SourceSchemaInjectableContext(schema_copy)):
                # configurations will be accessed in this section in the source
                proxy = Container()[PipelineContext]
                pipeline_name = None if not proxy.is_active() else proxy.pipeline().pipeline_name
                with inject_section(
                    ConfigSectionContext(
                        pipeline_name=pipeline_name,
                        sections=source_sections,
                        source_state_key=schema_copy.name,
                    )
                ):
                    rv = conf_f(*args, **kwargs)
                    return _eval_rv(rv, schema_copy)

        @wraps(conf_f)
        async def _wrap_coro(*args: Any, **kwargs: Any) -> TDltSourceImpl:
            """In case of co-routine we must wrap the whole injection context in awaitable,
            there's no easy way to avoid some code duplication
            """
            schema_copy = _make_schema()
            with Container().injectable_context(SourceSchemaInjectableContext(schema_copy)):
                # configurations will be accessed in this section in the source
                proxy = Container()[PipelineContext]
                pipeline_name = None if not proxy.is_active() else proxy.pipeline().pipeline_name
                with inject_section(
                    ConfigSectionContext(
                        pipeline_name=pipeline_name,
                        sections=source_sections,
                        source_state_key=schema_copy.name,
                    )
                ):
                    rv = await conf_f(*args, **kwargs)
                    return _eval_rv(rv, schema_copy)

        # get spec for wrapped function
        SPEC = get_fun_spec(conf_f)
        # get correct wrapper
        wrapper: AnyFun = _wrap_coro if inspect.iscoroutinefunction(inspect.unwrap(f)) else _wrap  # type: ignore[assignment]
        # store the source information
        _SOURCES[_wrap.__qualname__] = SourceInfo(SPEC, wrapper, func_module)
        if inspect.iscoroutinefunction(inspect.unwrap(f)):
            return _wrap_coro
        else:
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
    max_table_nesting: int = None,
    write_disposition: TTableHintTemplate[TWriteDispositionConfig] = None,
    columns: TTableHintTemplate[TAnySchemaColumns] = None,
    primary_key: TTableHintTemplate[TColumnNames] = None,
    merge_key: TTableHintTemplate[TColumnNames] = None,
    schema_contract: TTableHintTemplate[TSchemaContract] = None,
    table_format: TTableHintTemplate[TTableFormat] = None,
    file_format: TTableHintTemplate[TFileFormat] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    _impl_cls: Type[TDltResourceImpl] = DltResource,  # type: ignore[assignment]
) -> TDltResourceImpl: ...


@overload
def resource(
    data: None = ...,
    /,
    name: str = None,
    table_name: TTableHintTemplate[str] = None,
    max_table_nesting: int = None,
    write_disposition: TTableHintTemplate[TWriteDispositionConfig] = None,
    columns: TTableHintTemplate[TAnySchemaColumns] = None,
    primary_key: TTableHintTemplate[TColumnNames] = None,
    merge_key: TTableHintTemplate[TColumnNames] = None,
    schema_contract: TTableHintTemplate[TSchemaContract] = None,
    table_format: TTableHintTemplate[TTableFormat] = None,
    file_format: TTableHintTemplate[TFileFormat] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    _impl_cls: Type[TDltResourceImpl] = DltResource,  # type: ignore[assignment]
) -> Callable[[Callable[TResourceFunParams, Any]], TDltResourceImpl]: ...


@overload
def resource(
    data: None = ...,
    /,
    name: TTableHintTemplate[str] = None,
    table_name: TTableHintTemplate[str] = None,
    max_table_nesting: int = None,
    write_disposition: TTableHintTemplate[TWriteDispositionConfig] = None,
    columns: TTableHintTemplate[TAnySchemaColumns] = None,
    primary_key: TTableHintTemplate[TColumnNames] = None,
    merge_key: TTableHintTemplate[TColumnNames] = None,
    schema_contract: TTableHintTemplate[TSchemaContract] = None,
    table_format: TTableHintTemplate[TTableFormat] = None,
    file_format: TTableHintTemplate[TFileFormat] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    _impl_cls: Type[TDltResourceImpl] = DltResource,  # type: ignore[assignment]
    standalone: Literal[True] = True,
) -> Callable[
    [Callable[TResourceFunParams, Any]], Callable[TResourceFunParams, TDltResourceImpl]
]: ...


@overload
def resource(
    data: Union[List[Any], Tuple[Any], Iterator[Any]],
    /,
    name: str = None,
    table_name: TTableHintTemplate[str] = None,
    max_table_nesting: int = None,
    write_disposition: TTableHintTemplate[TWriteDispositionConfig] = None,
    columns: TTableHintTemplate[TAnySchemaColumns] = None,
    primary_key: TTableHintTemplate[TColumnNames] = None,
    merge_key: TTableHintTemplate[TColumnNames] = None,
    schema_contract: TTableHintTemplate[TSchemaContract] = None,
    table_format: TTableHintTemplate[TTableFormat] = None,
    file_format: TTableHintTemplate[TFileFormat] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    _impl_cls: Type[TDltResourceImpl] = DltResource,  # type: ignore[assignment]
) -> TDltResourceImpl: ...


def resource(
    data: Optional[Any] = None,
    /,
    name: TTableHintTemplate[str] = None,
    table_name: TTableHintTemplate[str] = None,
    max_table_nesting: int = None,
    write_disposition: TTableHintTemplate[TWriteDispositionConfig] = None,
    columns: TTableHintTemplate[TAnySchemaColumns] = None,
    primary_key: TTableHintTemplate[TColumnNames] = None,
    merge_key: TTableHintTemplate[TColumnNames] = None,
    schema_contract: TTableHintTemplate[TSchemaContract] = None,
    table_format: TTableHintTemplate[TTableFormat] = None,
    file_format: TTableHintTemplate[TFileFormat] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    _impl_cls: Type[TDltResourceImpl] = DltResource,  # type: ignore[assignment]
    standalone: bool = False,
    data_from: TUnboundDltResource = None,
) -> Any:
    """When used as a decorator, transforms any generator (yielding) function into a `dlt resource`. When used as a function, it transforms data in `data` argument into a `dlt resource`.

    #### Note:
    A `resource`is a location within a `source` that holds the data with specific structure (schema) or coming from specific origin. A resource may be a rest API endpoint, table in the database or a tab in Google Sheets.
    A `dlt resource` is python representation of a `resource` that combines both data and metadata (table schema) that describes the structure and instructs the loading of the data.
    A `dlt resource` is also an `Iterable` and can used like any other iterable object ie. list or tuple.

    Please refer to https://dlthub.com/docs/general-usage/resource for a complete documentation.

    #### Credentials:
    If used as a decorator (`data` argument is a `Generator`), it may automatically bind the source function arguments to the secret and config values.
    >>> @dlt.resource
    >>> def user_games(username, chess_url: str = dlt.config.value, api_secret = dlt.secrets.value):
    >>>     return requests.get("%s/games/%s" % (chess_url, username), headers={"Authorization": f"Bearer {api_secret}"})
    >>>
    >>> list(user_games("magnuscarlsen"))

    Here `username` is a required, explicit python argument, `chess_url` is a required argument, that if not explicitly passed will be taken from configuration ie. `config.toml`, `api_secret` is a required argument, that if not explicitly passed will be taken from dlt secrets ie. `secrets.toml`.
    See https://dlthub.com/docs/general-usage/credentials for details.
    Note that if decorated function is an inner function, passing of the credentials will be disabled.

    #### Args:
        data (Callable | Any, optional): a function to be decorated or a data compatible with `dlt` `run`.

        name (str, optional): A name of the resource that by default also becomes the name of the table to which the data is loaded.
        If not present, the name of the decorated function will be used.

        table_name (TTableHintTemplate[str], optional): An table name, if different from `name`.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        max_table_nesting (int, optional): A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.

        write_disposition (TTableHintTemplate[TWriteDispositionConfig], optional): Controls how to write data to a table. Accepts a shorthand string literal or configuration dictionary.
        Allowed shorthand string literals: `append` will always add new data at the end of the table. `replace` will replace existing data with new data. `skip` will prevent data from loading. "merge" will deduplicate and merge data based on "primary_key" and "merge_key" hints. Defaults to "append".
        Write behaviour can be further customized through a configuration dictionary. For example, to obtain an SCD2 table provide `write_disposition={"disposition": "merge", "strategy": "scd2"}`.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        columns (Sequence[TAnySchemaColumns], optional): A list, dict or pydantic model of column schemas.
            Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.
            This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.
            When the argument is a pydantic model, the model will be used to validate the data yielded by the resource as well.

        primary_key (str | Sequence[str]): A column name or a list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        merge_key (str | Sequence[str]): A column name or a list of column names that define a merge key. Typically used with "merge" write disposition to remove overlapping data ranges ie. to keep a single record for a given day.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        schema_contract (TSchemaContract, optional): Schema contract settings that will be applied to all resources of this source (if not overridden in the resource itself)

        table_format (Literal["iceberg", "delta"], optional): Defines the storage format of the table. Currently only "iceberg" is supported on Athena, and "delta" on the filesystem.
        Other destinations ignore this hint.

        file_format (Literal["preferred", ...], optional): Format of the file in which resource data is stored. Useful when importing external files. Use `preferred` to force
        a file format that is preferred by the destination used. This setting superseded the `load_file_format` passed to pipeline `run` method.

        selected (bool, optional): When `True` `dlt pipeline` will extract and load this resource, if `False`, the resource will be ignored.

        spec (Type[BaseConfiguration], optional): A specification of configuration and secret values required by the source.

        standalone (bool, optional): Returns a wrapped decorated function that creates DltResource instance. Must be called before use. Cannot be part of a source.

        data_from (TUnboundDltResource, optional): Allows to pipe data from one resource to another to build multi-step pipelines.

        parallelized (bool, optional): If `True`, the resource generator will be extracted in parallel with other resources. Defaults to `False`.

        _impl_cls (Type[TDltResourceImpl], optional): A custom implementation of DltResource, may be also used to providing just a typing stub

    Raises:
        ResourceNameMissing: indicates that name of the resource cannot be inferred from the `data` being passed.
        InvalidResourceDataType: indicates that the `data` argument cannot be converted into `dlt resource`

    Returns:
        TDltResourceImpl instance which may be loaded, iterated or combined with other resources into a pipeline.
    """

    def make_resource(_name: str, _section: str, _data: Any) -> TDltResourceImpl:
        table_template = make_hints(
            table_name,
            write_disposition=write_disposition or DEFAULT_WRITE_DISPOSITION,
            columns=columns,
            primary_key=primary_key,
            merge_key=merge_key,
            schema_contract=schema_contract,
            table_format=table_format,
            file_format=file_format,
        )

        resource = _impl_cls.from_data(
            _data,
            _name,
            _section,
            table_template,
            selected,
            cast(DltResource, data_from),
            True,
        )
        # If custom nesting level was specified then
        # we need to add it to table hints so that
        # later in normalizer dlt/common/normalizers/json/relational.py
        # we can override max_nesting level for the given table
        if max_table_nesting is not None:
            resource.max_table_nesting = max_table_nesting
        if parallelized:
            return resource.parallelize()
        return resource

    def decorator(
        f: Callable[TResourceFunParams, Any]
    ) -> Callable[TResourceFunParams, TDltResourceImpl]:
        if not callable(f):
            if data_from:
                # raise more descriptive exception if we construct transformer
                raise InvalidTransformerDataTypeGeneratorFunctionRequired(
                    name or "<no name>", f, type(f)
                )
            raise ResourceFunctionExpected(name or "<no name>", f, type(f))
        if not standalone and callable(name):
            raise DynamicNameNotStandaloneResource(get_callable_name(f))

        resource_name = name if name and not callable(name) else get_callable_name(f)

        func_module = inspect.getmodule(f)
        source_section = _get_source_section_name(func_module)
        is_inner_resource = is_inner_callable(f)

        if spec is None:
            # autodetect spec
            SPEC, resolvable_fields = spec_from_signature(
                f, inspect.signature(f), include_defaults=standalone
            )
            if is_inner_resource and not standalone:
                if len(resolvable_fields) > 0:
                    # prevent required arguments to inner functions that are not standalone
                    raise ResourceInnerCallableConfigWrapDisallowed(resource_name, source_section)
                else:
                    # empty spec for inner functions - they should not be injected
                    SPEC = BaseConfiguration
        else:
            SPEC = spec
        # assign spec to "f"
        set_fun_spec(f, SPEC)

        # store the non-inner resource information
        if not is_inner_resource:
            _SOURCES[f.__qualname__] = SourceInfo(SPEC, f, func_module)

        if not standalone:
            # we return a DltResource that is callable and returns dlt resource when called
            # so it should match the signature
            return make_resource(resource_name, source_section, f)  # type: ignore[return-value]

        @wraps(f)
        def _wrap(*args: Any, **kwargs: Any) -> TDltResourceImpl:
            skip_args = 1 if data_from else 0
            _, mod_sig, bound_args = simulate_func_call(f, skip_args, *args, **kwargs)
            actual_resource_name = name(bound_args.arguments) if callable(name) else resource_name
            r = make_resource(actual_resource_name, source_section, f)
            # wrap the standalone resource
            data_ = r._pipe.bind_gen(*args, **kwargs)
            if isinstance(data_, DltResource):
                # we allow an edge case: resource can return another resource
                r = data_  # type: ignore[assignment]
            # consider transformer arguments bound
            r._args_bound = True
            # keep explicit args passed
            r._set_explicit_args(f, mod_sig, *args, **kwargs)
            return r

        return _wrap

    # if data is callable or none use decorator
    if data is None:
        # we're called with parens.
        return decorator

    if callable(data):
        return decorator(data)
    else:
        # take name from the generator
        source_section: str = None
        if inspect.isgenerator(data):
            name = name or get_callable_name(data)  # type: ignore
            func_module = inspect.getmodule(data.gi_frame)
            source_section = _get_source_section_name(func_module)
        assert not callable(name)
        return make_resource(name, source_section, data)


@overload
def transformer(
    f: None = ...,
    /,
    data_from: TUnboundDltResource = DltResource.Empty,
    name: str = None,
    table_name: TTableHintTemplate[str] = None,
    max_table_nesting: int = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TAnySchemaColumns] = None,
    primary_key: TTableHintTemplate[TColumnNames] = None,
    merge_key: TTableHintTemplate[TColumnNames] = None,
    schema_contract: TTableHintTemplate[TSchemaContract] = None,
    table_format: TTableHintTemplate[TTableFormat] = None,
    file_format: TTableHintTemplate[TFileFormat] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
) -> Callable[[Callable[Concatenate[TDataItem, TResourceFunParams], Any]], DltResource]: ...


@overload
def transformer(
    f: None = ...,
    /,
    data_from: TUnboundDltResource = DltResource.Empty,
    name: TTableHintTemplate[str] = None,
    table_name: TTableHintTemplate[str] = None,
    max_table_nesting: int = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TAnySchemaColumns] = None,
    primary_key: TTableHintTemplate[TColumnNames] = None,
    merge_key: TTableHintTemplate[TColumnNames] = None,
    schema_contract: TTableHintTemplate[TSchemaContract] = None,
    table_format: TTableHintTemplate[TTableFormat] = None,
    file_format: TTableHintTemplate[TFileFormat] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    standalone: Literal[True] = True,
) -> Callable[
    [Callable[Concatenate[TDataItem, TResourceFunParams], Any]],
    Callable[TResourceFunParams, DltResource],
]: ...


@overload
def transformer(
    f: Callable[Concatenate[TDataItem, TResourceFunParams], Any],
    /,
    data_from: TUnboundDltResource = DltResource.Empty,
    name: str = None,
    table_name: TTableHintTemplate[str] = None,
    max_table_nesting: int = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TAnySchemaColumns] = None,
    primary_key: TTableHintTemplate[TColumnNames] = None,
    merge_key: TTableHintTemplate[TColumnNames] = None,
    schema_contract: TTableHintTemplate[TSchemaContract] = None,
    table_format: TTableHintTemplate[TTableFormat] = None,
    file_format: TTableHintTemplate[TFileFormat] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
) -> DltResource: ...


@overload
def transformer(
    f: Callable[Concatenate[TDataItem, TResourceFunParams], Any],
    /,
    data_from: TUnboundDltResource = DltResource.Empty,
    name: TTableHintTemplate[str] = None,
    table_name: TTableHintTemplate[str] = None,
    max_table_nesting: int = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TAnySchemaColumns] = None,
    primary_key: TTableHintTemplate[TColumnNames] = None,
    merge_key: TTableHintTemplate[TColumnNames] = None,
    schema_contract: TTableHintTemplate[TSchemaContract] = None,
    table_format: TTableHintTemplate[TTableFormat] = None,
    file_format: TTableHintTemplate[TFileFormat] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    standalone: Literal[True] = True,
) -> Callable[TResourceFunParams, DltResource]: ...


def transformer(
    f: Optional[Callable[Concatenate[TDataItem, TResourceFunParams], Any]] = None,
    /,
    data_from: TUnboundDltResource = DltResource.Empty,
    name: TTableHintTemplate[str] = None,
    table_name: TTableHintTemplate[str] = None,
    max_table_nesting: int = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TAnySchemaColumns] = None,
    primary_key: TTableHintTemplate[TColumnNames] = None,
    merge_key: TTableHintTemplate[TColumnNames] = None,
    schema_contract: TTableHintTemplate[TSchemaContract] = None,
    table_format: TTableHintTemplate[TTableFormat] = None,
    file_format: TTableHintTemplate[TFileFormat] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    standalone: bool = False,
    _impl_cls: Type[TDltResourceImpl] = DltResource,  # type: ignore[assignment]
) -> Any:
    """A form of `dlt resource` that takes input from other resources via `data_from` argument in order to enrich or transform the data.

    The decorated function `f` must take at least one argument of type TDataItems (a single item or list of items depending on the resource `data_from`). `dlt` will pass
    metadata associated with the data item if argument with name `meta` is present. Otherwise, transformer function may take more arguments and be parametrized
    like the resources.

    You can bind the transformer early by specifying resource in `data_from` when the transformer is created or create dynamic bindings later with | operator
    which is demonstrated in example below:

    Example:
    >>> @dlt.resource
    >>> def players(title, chess_url=dlt.config.value):
    >>>     r = requests.get(f"{chess_url}titled/{title}")
    >>>     yield r.json()["players"]  # returns list of player names
    >>>
    >>> # this resource takes data from players and returns profiles
    >>> @dlt.transformer(write_disposition="replace")
    >>> def player_profile(player: Any) -> Iterator[TDataItems]:
    >>>     r = requests.get(f"{chess_url}player/{player}")
    >>>     r.raise_for_status()
    >>>     yield r.json()
    >>>
    >>> # pipes the data from players into player profile to produce a list of player profiles
    >>> list(players("GM") | player_profile)

    Args:
        f: (Callable): a function taking minimum one argument of TDataItems type which will receive data yielded from `data_from` resource.

        data_from (Callable | Any, optional): a resource that will send data to the decorated function `f`

        name (str, optional): A name of the resource that by default also becomes the name of the table to which the data is loaded.
        If not present, the name of the decorated function will be used.

        table_name (TTableHintTemplate[str], optional): An table name, if different from `name`.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        max_table_nesting (int, optional): A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.

        write_disposition (Literal["skip", "append", "replace", "merge"], optional): Controls how to write data to a table. `append` will always add new data at the end of the table. `replace` will replace existing data with new data. `skip` will prevent data from loading. "merge" will deduplicate and merge data based on "primary_key" and "merge_key" hints. Defaults to "append".
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        columns (Sequence[TAnySchemaColumns], optional): A list, dict or pydantic model of column schemas. Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        primary_key (str | Sequence[str]): A column name or a list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        merge_key (str | Sequence[str]): A column name or a list of column names that define a merge key. Typically used with "merge" write disposition to remove overlapping data ranges ie. to keep a single record for a given day.
        This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        schema_contract (TSchemaContract, optional): Schema contract settings that will be applied to all resources of this source (if not overridden in the resource itself)

        table_format (Literal["iceberg", "delta"], optional): Defines the storage format of the table. Currently only "iceberg" is supported on Athena, and "delta" on the filesystem.
        Other destinations ignore this hint.

        file_format (Literal["preferred", ...], optional): Format of the file in which resource data is stored. Useful when importing external files. Use `preferred` to force
        a file format that is preferred by the destination used. This setting superseded the `load_file_format` passed to pipeline `run` method.

        selected (bool, optional): When `True` `dlt pipeline` will extract and load this resource, if `False`, the resource will be ignored.

        spec (Type[BaseConfiguration], optional): A specification of configuration and secret values required by the source.

        standalone (bool, optional): Returns a wrapped decorated function that creates DltResource instance. Must be called before use. Cannot be part of a source.

        _impl_cls (Type[TDltResourceImpl], optional): A custom implementation of DltResource, may be also used to providing just a typing stub
    """
    if isinstance(f, DltResource):
        raise ValueError(
            "Please pass `data_from=` argument as keyword argument. The only positional argument to"
            " transformer is the decorated function"
        )

    return resource(  # type: ignore
        f,
        name=name,
        table_name=table_name,
        max_table_nesting=max_table_nesting,
        write_disposition=write_disposition,
        columns=columns,
        primary_key=primary_key,
        merge_key=merge_key,
        schema_contract=schema_contract,
        table_format=table_format,
        file_format=file_format,
        selected=selected,
        spec=spec,
        standalone=standalone,
        data_from=data_from,
        parallelized=parallelized,
        _impl_cls=_impl_cls,
    )


def _maybe_load_schema_for_callable(f: AnyFun, name: str) -> Optional[Schema]:
    if not inspect.isfunction(f):
        f = f.__class__
    try:
        file = inspect.getsourcefile(f)
        if file:
            schema = SchemaStorage.load_schema_file(
                os.path.dirname(file), name, remove_processing_hints=True
            )
            schema.update_normalizers()
            return schema
    except SchemaNotFoundError:
        pass
    return None


def _get_source_section_name(m: ModuleType) -> str:
    """Gets the source section name (as in SOURCES <section> <name> tuple) from __source_name__ of the module `m` or from its name"""
    if m is None:
        return None
    if hasattr(m, "__source_name__"):
        return cast(str, m.__source_name__)
    return get_module_name(m)


def get_source_schema() -> Schema:
    """When executed from the function decorated with @dlt.source, returns a writable source Schema"""
    try:
        return Container()[SourceSchemaInjectableContext].schema
    except ContextDefaultCannotBeCreated:
        raise CurrentSourceSchemaNotAvailable()


def get_source() -> DltSource:
    """When executed from the function decorated with @dlt.resource, returns currently extracted source"""
    try:
        return Container()[SourceInjectableContext].source
    except ContextDefaultCannotBeCreated:
        raise CurrentSourceNotAvailable()


TBoundItems = TypeVar("TBoundItems", bound=TDataItems)
TDeferred = Callable[[], TBoundItems]
TDeferredFunParams = ParamSpec("TDeferredFunParams")


def defer(
    f: Callable[TDeferredFunParams, TBoundItems]
) -> Callable[TDeferredFunParams, TDeferred[TBoundItems]]:
    @wraps(f)
    def _wrap(*args: Any, **kwargs: Any) -> TDeferred[TBoundItems]:
        def _curry() -> TBoundItems:
            return f(*args, **kwargs)

        return _curry

    return _wrap
