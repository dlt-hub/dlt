import os
import inspect
from types import ModuleType
from functools import update_wrapper, wraps
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Dict,
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
from typing_extensions import TypeVar, Self

from dlt.common import logger
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
from dlt.common.reflection.inspect import iscoroutinefunction
from dlt.common.schema.utils import DEFAULT_WRITE_DISPOSITION
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import (
    TFileFormat,
    TWriteDisposition,
    TWriteDispositionConfig,
    TAnySchemaColumns,
    TSchemaContract,
    TTableFormat,
    TTableReferenceParam,
)
from dlt.common.storages.exceptions import SchemaNotFoundError
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.typing import (
    AnyFun,
    ParamSpec,
    Generic,
    Concatenate,
    TDataItem,
    TDataItems,
    TColumnNames,
    TTableNames,
)
from dlt.common.utils import (
    get_callable_name,
    get_module_name,
    is_inner_callable,
    get_full_callable_name,
)

from dlt.extract.hints import TResourceNestedHints, make_hints
from dlt.extract.utils import dynstr
from dlt.extract.exceptions import (
    CurrentSourceNotAvailable,
    InvalidTransformerDataTypeGeneratorFunctionRequired,
    ResourceFunctionExpected,
    SourceDataIsNone,
    SourceIsAClassTypeError,
    ExplicitSourceNameInvalid,
    SourceNotAFunction,
    CurrentSourceSchemaNotAvailable,
)
from dlt.extract.items import TTableHintTemplate
from dlt.extract.source import DltSource
from dlt.extract.reference import SourceReference, SourceFactory, TDltSourceImpl, TSourceFunParams
from dlt.extract.resource import DltResource, TUnboundDltResource, TDltResourceImpl
from dlt.extract.incremental import TIncrementalConfig


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


class _DltSingleSource(DltSource):
    """Used to register standalone (non-inner) resources"""

    @property
    def single_resource(self) -> DltResource:
        return list(self.resources.values())[0]


class DltSourceFactoryWrapper(SourceFactory[TSourceFunParams, TDltSourceImpl]):
    def __init__(
        self,
    ) -> None:
        """Creates a wrapper that is returned by @source decorator. It preserves the decorated function when called and
        allows to change the decorator arguments at runtime. Changing the `name` and `section` creates a clone of the source
        with different name and taking the configuration from a different keys.

        This wrapper registers the source under `section`.`name` type in SourceReference registry, using the original
        `section` (which corresponds to module name) and `name` (which corresponds to source function name).
        """
        self._f: AnyFun = None
        self.ref: SourceReference = None
        self._deco_f: Callable[..., TDltSourceImpl] = None

        self.name: str = None
        self.section: str = None
        self.max_table_nesting: int = None
        self.root_key: bool = False
        self.schema: Schema = None
        self.schema_contract: TSchemaContract = None
        self.spec: Type[BaseConfiguration] = None
        self.parallelized: bool = None
        self._impl_cls: Type[TDltSourceImpl] = DltSource  # type: ignore[assignment]

    def clone(
        self,
        *,
        name: str = None,
        section: str = None,
        max_table_nesting: int = None,
        root_key: bool = None,
        schema: Schema = None,
        schema_contract: TSchemaContract = None,
        spec: Type[BaseConfiguration] = None,
        parallelized: bool = None,
        _impl_cls: Type[TDltSourceImpl] = None,
    ) -> Self:
        """Overrides default arguments that will be used to create DltSource instance when this wrapper is called. This method
        clones this wrapper.
        """
        # if source function not set, apply args in place
        ovr = self.__class__() if self._f else self

        if name is not None:
            ovr.name = name
        else:
            ovr.name = self.name
        if section is not None:
            ovr.section = section
        else:
            ovr.section = self.section
        if max_table_nesting is not None:
            ovr.max_table_nesting = max_table_nesting
        else:
            ovr.max_table_nesting = self.max_table_nesting
        if root_key is not None:
            ovr.root_key = root_key
        else:
            ovr.root_key = self.root_key
        ovr.schema = schema or self.schema
        if schema_contract is not None:
            ovr.schema_contract = schema_contract
        else:
            ovr.schema_contract = self.schema_contract
        ovr.spec = spec or self.spec
        if parallelized is not None:
            ovr.parallelized = parallelized
        else:
            ovr.parallelized = self.parallelized
        ovr._impl_cls = _impl_cls or self._impl_cls

        # also remember original source function
        ovr._f = self._f
        ovr.ref = self.ref
        ovr._update_wrapper()
        # try to bind _f
        ovr.wrap()
        return ovr

    with_args = clone

    def __call__(self, *args: Any, **kwargs: Any) -> TDltSourceImpl:
        assert self._deco_f, f"Attempt to call source function on {self.name} before bind"
        # if source impl is a single resource source
        if issubclass(self._impl_cls, _DltSingleSource):
            # call special source function that will create renamed resource
            source = self._deco_f(self.name, self.section, args, kwargs)
            assert isinstance(source, _DltSingleSource)
            # set source section to empty to not interfere with resource sections, same thing we do in extract
            source.section = source.single_resource.section
            # apply selected settings directly to resource
            resource = source.single_resource
            if self.max_table_nesting is not None:
                resource.max_table_nesting = self.max_table_nesting
            if self.schema_contract is not None:
                resource.apply_hints(schema_contract=self.schema_contract)
        else:
            source = self._deco_f(*args, **kwargs)
        return source

    def bind(self, f: AnyFun) -> Self:
        """Binds wrapper to the original source function and registers the source reference. This method is called only once by the decorator"""
        self._f = f
        self._update_wrapper()
        self.ref = self.wrap()
        if not is_inner_callable(f):
            SourceReference.register(self.ref)
        return self

    def wrap(self) -> SourceReference:
        """Wrap the original source function using _deco."""
        if not self._f:
            return None
        if hasattr(self._f, "__qualname__"):
            self.__qualname__ = self._f.__qualname__
        return self._wrap(self._f)

    def _update_wrapper(self) -> None:
        """wrap self so we preserve signature, module etc. from f"""
        if not callable(self._f):
            return None
        update_wrapper(self, self._f)
        # also change the signature return annotation to dlt source class
        sig = inspect.signature(self._f).replace(return_annotation=self._impl_cls)
        self.__signature__ = sig

    def _wrap(self, f: AnyFun) -> SourceReference:
        """Wraps source function `f` in configuration injector."""
        if not callable(f) or isinstance(f, DltResource):
            raise SourceNotAFunction(self.name or "<no name>", f, type(f))

        if inspect.isclass(f):
            raise SourceIsAClassTypeError(self.name or "<no name>", f)

        # source name is passed directly or taken from decorated function name
        effective_name = self.name or get_callable_name(f)

        if self.schema and self.name and self.name != self.schema.name:
            raise ExplicitSourceNameInvalid(self.name, self.schema.name)

        # wrap source extraction function in configuration with section
        func_module = inspect.getmodule(f)
        source_section = self.section or _get_source_section_name(func_module)
        # use effective_name which is explicit source name or callable name to represent third element in source config path
        source_sections = (known_sections.SOURCES, source_section, effective_name)
        conf_f = with_config(f, spec=self.spec, sections=source_sections)

        def _eval_rv(_rv: Any, schema_copy: Schema) -> TDltSourceImpl:
            """Evaluates return value from the source function or coroutine"""
            if _rv is None:
                raise SourceDataIsNone(schema_copy.name)
            # if generator, consume it immediately
            if inspect.isgenerator(_rv):
                _rv = list(_rv)

            # convert to source
            s = self._impl_cls.from_data(schema_copy, source_section, _rv)
            # apply hints
            if self.max_table_nesting is not None:
                s.max_table_nesting = self.max_table_nesting
            s.schema_contract = self.schema_contract
            # enable root propagation
            s.root_key = self.root_key
            # parallelize resources
            if self.parallelized:
                s.parallelize()
            return s

        def _make_schema() -> Schema:
            if not self.schema:
                # load the schema from file with name_schema.yaml/json from the same directory, the callable resides OR create new default schema
                return _maybe_load_schema_for_callable(f, effective_name) or Schema(effective_name)
            else:
                # clone the schema passed to decorator, update normalizers, remove processing hints
                # NOTE: source may be called several times in many different settings
                return self.schema.clone(update_normalizers=True, remove_processing_hints=True)

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
        self._deco_f = _wrap_coro if iscoroutinefunction(f) else _wrap  # type: ignore[assignment]
        return SourceReference(get_full_callable_name(f), SPEC, self, source_section, effective_name)  # type: ignore[arg-type]


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
    schema_contract: TSchemaContract = None,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    _impl_cls: Type[TDltSourceImpl] = DltSource,  # type: ignore[assignment]
) -> SourceFactory[TSourceFunParams, TDltSourceImpl]: ...


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
    parallelized: bool = False,
    _impl_cls: Type[TDltSourceImpl] = DltSource,  # type: ignore[assignment]
) -> Callable[
    [Callable[TSourceFunParams, Any]], SourceFactory[TSourceFunParams, TDltSourceImpl]
]: ...


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
    parallelized: bool = False,
    _impl_cls: Type[TDltSourceImpl] = DltSource,  # type: ignore[assignment]
) -> Any:
    """A decorator that transforms a function returning one or more `dlt resources` into a `dlt source` in order to load it with `dlt`.

    Note:
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
    See https://dlthub.com/docs/general-usage/credentials/ for details.

    Args:
        func (Optional[AnyFun]): A function that returns a dlt resource or a list of those or a list of any data items that can be loaded by `dlt`.

        name (str, optional): A name of the source which is also the name of the associated schema. If not present, the function name will be used.

        section (str, optional): Configuration section that comes right after 'sources` in default layout. If not present, the current python module name will be used.
            Default layout is `sources.<section>.<name>.<key_name>`.

        max_table_nesting (int, optional): A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.

        root_key (bool): Enables merging on all resources by propagating row key from root to all nested tables. This option is most useful if you plan to change write disposition of a resource to disable/enable merge. Defaults to False.

        schema (Schema, optional): An explicit `Schema` instance to be associated with the source. If not present, `dlt` creates a new `Schema` object with provided `name`. If such `Schema` already exists in the same folder as the module containing the decorated function, such schema will be loaded from file.

        schema_contract (TSchemaContract, optional): Schema contract settings that will be applied to this resource.

        spec (Type[BaseConfiguration], optional): A specification of configuration and secret values required by the source.

        parallelized (bool, optional): If `True`, resource generators will be extracted in parallel with other resources.
            Transformers that return items are also parallelized. Non-eligible resources are ignored. Defaults to `False` which preserves resource settings.

        _impl_cls (Type[TDltSourceImpl], optional): A custom implementation of DltSource, may be also used to providing just a typing stub

    Returns:
        Any: Wrapped decorated source function, see SourceFactory reference for additional wrapper capabilities
    """
    if name and schema:
        raise ArgumentsOverloadException(
            "`name` has no effect when `schema` argument is present", source.__name__
        )

    source_wrapper = (
        DltSourceFactoryWrapper[Any, TDltSourceImpl]()
        .clone(
            name=name,
            section=section,
            max_table_nesting=max_table_nesting,
            root_key=root_key,
            schema=schema,
            schema_contract=schema_contract,
            spec=spec,
            parallelized=parallelized,
            _impl_cls=_impl_cls,
        )
        .bind
    )

    if func is None:
        # we're called with parens.
        return source_wrapper
    # we're called as @source without parens.
    return source_wrapper(func)


class ResourceFactory(DltResource, Generic[TResourceFunParams, TDltResourceImpl]):
    # this class is used only for typing, do not instantiate, do not add docstring
    def __call__(  # type: ignore[override]
        self, *args: TResourceFunParams.args, **kwargs: TResourceFunParams.kwargs
    ) -> TDltResourceImpl:
        pass


@overload
def resource(
    data: Callable[TResourceFunParams, Any],
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
    references: TTableHintTemplate[TTableReferenceParam] = None,
    nested_hints: Optional[TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]]] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    incremental: Optional[TIncrementalConfig] = None,
    _impl_cls: Type[TDltResourceImpl] = DltResource,  # type: ignore[assignment]
    section: Optional[TTableHintTemplate[str]] = None,
    _base_spec: Type[BaseConfiguration] = BaseConfiguration,
    standalone: bool = None,
) -> ResourceFactory[TResourceFunParams, TDltResourceImpl]: ...


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
    references: TTableHintTemplate[TTableReferenceParam] = None,
    nested_hints: Optional[TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]]] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    incremental: Optional[TIncrementalConfig] = None,
    _impl_cls: Type[TDltResourceImpl] = DltResource,  # type: ignore[assignment]
    section: Optional[TTableHintTemplate[str]] = None,
    _base_spec: Type[BaseConfiguration] = BaseConfiguration,
    standalone: bool = None,
) -> Callable[
    [Callable[TResourceFunParams, Any]], ResourceFactory[TResourceFunParams, TDltResourceImpl]
]: ...


@overload
def resource(
    data: Union[List[Any], Iterator[Any]],
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
    references: TTableHintTemplate[TTableReferenceParam] = None,
    nested_hints: Optional[TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]]] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    incremental: Optional[TIncrementalConfig] = None,
    _impl_cls: Type[TDltResourceImpl] = DltResource,  # type: ignore[assignment]
    section: Optional[str] = None,
    _base_spec: Type[BaseConfiguration] = BaseConfiguration,
    standalone: bool = None,
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
    references: TTableHintTemplate[TTableReferenceParam] = None,
    nested_hints: Optional[TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]]] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    incremental: Optional[TIncrementalConfig] = None,
    _impl_cls: Type[TDltResourceImpl] = DltResource,  # type: ignore[assignment]
    section: Optional[TTableHintTemplate[str]] = None,
    _base_spec: Type[BaseConfiguration] = BaseConfiguration,
    standalone: bool = None,
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
    See https://dlthub.com/docs/general-usage/credentials/ for details.
    Note that if decorated function is an inner function, passing of the credentials will be disabled.

    Args:
        data (Optional[Any], optional): a function to be decorated or a data compatible with `dlt` `run`.

        name (TTableHintTemplate[str], optional): A name of the resource that by default also becomes the name of the table to which the data is loaded.
            If not present, the name of the decorated function will be used.

        table_name (TTableHintTemplate[str], optional): An table name, if different from `name`.
            This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        max_table_nesting (int, optional): A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.

        write_disposition (TTableHintTemplate[TWriteDispositionConfig], optional): Controls how to write data to a table. Accepts a shorthand string literal or configuration dictionary.
            Allowed shorthand string literals: `append` will always add new data at the end of the table. `replace` will replace existing data with new data. `skip` will prevent data from loading. "merge" will deduplicate and merge data based on "primary_key" and "merge_key" hints. Defaults to "append".
            Write behaviour can be further customized through a configuration dictionary. For example, to obtain an SCD2 table provide `write_disposition={"disposition": "merge", "strategy": "scd2"}`.
            This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        columns (TTableHintTemplate[TAnySchemaColumns], optional): A list, dict or pydantic model of column schemas.
            Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.
            This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.
            When the argument is a pydantic model, the model will be used to validate the data yielded by the resource as well.

        primary_key (TTableHintTemplate[TColumnNames], optional): A column name or a list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.
            This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        merge_key (TTableHintTemplate[TColumnNames], optional): A column name or a list of column names that define a merge key. Typically used with "merge" write disposition to remove overlapping data ranges ie. to keep a single record for a given day.
            This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        schema_contract (TTableHintTemplate[TSchemaContract], optional): Schema contract settings that will be applied to all resources of this source (if not overridden in the resource itself)

        table_format (TTableHintTemplate[TTableFormat], optional): Defines the storage format of the table. Currently only "iceberg" is supported on Athena, and "delta" on the filesystem.
            Other destinations ignore this hint.

        file_format (TTableHintTemplate[TFileFormat], optional): Format of the file in which resource data is stored. Useful when importing external files. Use `preferred` to force
            a file format that is preferred by the destination used. This setting superseded the `load_file_format` passed to pipeline `run` method.

        references (TTableHintTemplate[TTableReferenceParam], optional): A list of references to other table's columns.
            A list in the form of `[{'referenced_table': 'other_table', 'columns': ['other_col1', 'other_col2'], 'referenced_columns': ['col1', 'col2']}]`.
            Table and column names will be normalized according to the configured naming convention.

        nested_hints (Optional[TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]]], optional): Hints for nested tables created by this resource.

        selected (bool, optional): When `True` `dlt pipeline` will extract and load this resource, if `False`, the resource will be ignored.

        spec (Type[BaseConfiguration], optional): A specification of configuration and secret values required by the source.

        parallelized (bool, optional): If `True`, the resource generator will be extracted in parallel with other resources.
            Transformers that return items are also parallelized. Defaults to `False`.

        incremental (Optional[TIncrementalConfig], optional): An incremental configuration for the resource.

        _impl_cls (Type[TDltResourceImpl], optional): A custom implementation of DltResource, may be also used to providing just a typing stub

        section (Optional[TTableHintTemplate[str]], optional): Configuration section that comes right after 'sources` in default layout. If not present, the current python module name will be used.
            Default layout is `sources.<section>.<name>.<key_name>`. Note that resource section is used only when a single resource is passed to the pipeline.

        _base_spec (Type[BaseConfiguration], optional): A base spec used to which spec derived from resource function arguments is added

        standalone (bool, optional): Deprecated. Past functionality got merged into regular resource

        data_from (TUnboundDltResource, optional): Allows to pipe data from one resource to another to build multi-step pipelines.

    Raises:
        ResourceNameMissing: indicates that name of the resource cannot be inferred from the `data` being passed.
        InvalidResourceDataType: indicates that the `data` argument cannot be converted into `dlt resource`

    Returns:
        Any: TDltResourceImpl instance which may be loaded, iterated or combined with other resources into a pipeline.
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
            references=references,
            nested_hints=nested_hints,
            incremental=incremental,
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

        if incremental:
            # Reset the flag to allow overriding by incremental argument
            resource.incremental._from_hints = False
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
        f: Callable[TResourceFunParams, Any],
    ) -> TDltResourceImpl:
        if not callable(f):
            if data_from:
                # raise more descriptive exception if we construct transformer
                raise InvalidTransformerDataTypeGeneratorFunctionRequired(
                    name or "<no name>", f, type(f)
                )
            raise ResourceFunctionExpected(name or "<no name>", f, type(f))

        # allow for name and section to be created at runtime via dynstr which will
        # be evaluated when resource will be parametrized/called
        func_module = inspect.getmodule(f)
        if callable(name):
            resource_name: str = dynstr(get_callable_name(f), name)
        else:
            resource_name = name or get_callable_name(f)
        if callable(section):
            source_section: str = dynstr(_get_source_section_name(func_module), section)
        else:
            source_section = section or _get_source_section_name(func_module)

        is_inner_resource = is_inner_callable(f)
        if spec is None:
            # autodetect spec
            SPEC, resolvable_fields = spec_from_signature(f, inspect.signature(f), base=_base_spec)
        else:
            SPEC, resolvable_fields = spec, spec.get_resolvable_fields()

        # add spec to f
        set_fun_spec(f, SPEC)

        if is_inner_resource and len(resolvable_fields) > 0 and data_from is not None:
            # warn for parametrized inner transformers due to injection costs
            # we do not warn on top level ones because that's normal pattern here
            logger.info(
                f"Transformer {resource_name} in section {source_section} is defined over an inner"
                " function and has additional arguments that will be injected from the"
                " configuration. Note that transformers  are called for each data item (typically"
                " batch or page) and configuration injection is costly .Use the dlt.source to get"
                " the required configuration and pass them explicitly to your source."
            )

        factory = None
        r_ = make_resource(resource_name, source_section, f)
        # register non inner resources as source with single resource in it
        if not is_inner_resource:
            # a source function for the source wrapper, args that go to source are forwarded
            # to a single resource within
            def _source(
                name_ovr: str, section_ovr: str, args: Tuple[Any, ...], kwargs: Dict[str, Any]
            ) -> DltResource:
                return r_.with_name(name_ovr, new_section=section_ovr)(*args, **kwargs)

            # make the source module same as original resource
            _source.__qualname__ = f.__qualname__
            _source.__module__ = f.__module__
            # setup our special single resource source
            factory = (
                DltSourceFactoryWrapper[Any, DltSource]()
                .clone(
                    name=resource_name,
                    section=source_section,
                    spec=_base_spec,
                    _impl_cls=_DltSingleSource,
                )
                .bind(_source)
            )
            # mod the reference to keep the right spec
            factory.ref.SPEC = SPEC

        # associate source factory with the decorated function for the standalone=True resource
        # this provides access to standalone resources in the same way as to sources via SourceReference
        r_._factory = factory  # type: ignore[attr-defined]

        return r_

    # if data is callable or none use decorator
    if data is None:
        # we're called with parens.
        return decorator

    if callable(data):
        return decorator(data)
    else:
        assert not callable(name)
        assert not callable(section)

        # take name from the generator
        source_section: str = None
        if inspect.isgenerator(data):
            name = name or get_callable_name(data)  # type: ignore
            func_module = inspect.getmodule(data.gi_frame)
            source_section = section or _get_source_section_name(func_module)
        return make_resource(name, source_section, data)


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
    references: TTableHintTemplate[TTableReferenceParam] = None,
    nested_hints: Optional[TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]]] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    section: Optional[TTableHintTemplate[str]] = None,
    standalone: bool = None,
) -> Callable[
    [Callable[Concatenate[TDataItem, TResourceFunParams], Any]],
    ResourceFactory[TResourceFunParams, DltResource],
]: ...


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
    references: TTableHintTemplate[TTableReferenceParam] = None,
    nested_hints: Optional[TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]]] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    section: Optional[TTableHintTemplate[str]] = None,
    standalone: bool = None,
) -> ResourceFactory[TResourceFunParams, DltResource]: ...


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
    references: TTableHintTemplate[TTableReferenceParam] = None,
    nested_hints: Optional[TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]]] = None,
    selected: bool = True,
    spec: Type[BaseConfiguration] = None,
    parallelized: bool = False,
    section: Optional[TTableHintTemplate[str]] = None,
    standalone: bool = None,
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
        f (Optional[Callable[Concatenate[TDataItem, TResourceFunParams], Any]]): a function taking minimum one argument of TDataItems type which will receive data yielded from `data_from` resource.

        data_from (TUnboundDltResource, optional): a resource that will send data to the decorated function `f`

        name (TTableHintTemplate[str], optional): A name of the resource that by default also becomes the name of the table to which the data is loaded.
            If not present, the name of the decorated function will be used.

        table_name (TTableHintTemplate[str], optional): An table name, if different from `name`.
            This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        max_table_nesting (int, optional): A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.

        write_disposition (TTableHintTemplate[TWriteDisposition], optional): Controls how to write data to a table. `append` will always add new data at the end of the table. `replace` will replace existing data with new data. `skip` will prevent data from loading. "merge" will deduplicate and merge data based on "primary_key" and "merge_key" hints. Defaults to "append".
            This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        columns (TTableHintTemplate[TAnySchemaColumns], optional): A list, dict or pydantic model of column schemas. Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.
            This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        primary_key (TTableHintTemplate[TColumnNames], optional): A column name or a list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.
            This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        merge_key (TTableHintTemplate[TColumnNames], optional): A column name or a list of column names that define a merge key. Typically used with "merge" write disposition to remove overlapping data ranges ie. to keep a single record for a given day.
            This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.

        schema_contract (TTableHintTemplate[TSchemaContract], optional): Schema contract settings that will be applied to all resources of this source (if not overridden in the resource itself)

        table_format (TTableHintTemplate[TTableFormat], optional): Defines the storage format of the table. Currently only "iceberg" is supported on Athena, and "delta" on the filesystem.
            Other destinations ignore this hint.

        file_format (TTableHintTemplate[TFileFormat], optional): Format of the file in which resource data is stored. Useful when importing external files. Use `preferred` to force
            a file format that is preferred by the destination used. This setting superseded the `load_file_format` passed to pipeline `run` method.

        references (TTableHintTemplate[TTableReferenceParam], optional): A list of references to other table's columns.
            A list in the form of `[{'referenced_table': 'other_table', 'columns': ['other_col1', 'other_col2'], 'referenced_columns': ['col1', 'col2']}]`.
            Table and column names will be normalized according to the configured naming convention.

        nested_hints (Optional[TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]]], optional): Hints for nested tables created by this resource.

        selected (bool, optional): When `True` `dlt pipeline` will extract and load this resource, if `False`, the resource will be ignored.

        spec (Type[BaseConfiguration], optional): A specification of configuration and secret values required by the source.

        parallelized (bool, optional): When `True` the resource will be loaded in parallel.

        section (Optional[TTableHintTemplate[str]], optional): Configuration section that comes right after 'sources` in default layout. If not present, the current python module name will be used.
            Default layout is `sources.<section>.<name>.<key_name>`. Note that resource section is used only when a single resource is passed to the pipeline.

        standalone (bool, optional): Deprecated. Past functionality got merged into regular resource

        _impl_cls (Type[TDltResourceImpl], optional): A custom implementation of DltResource, may be also used to providing just a typing stub

    Raises:
        ResourceNameMissing: indicates that name of the resource cannot be inferred from the `data` being passed.
        InvalidResourceDataType: indicates that the `data` argument cannot be converted into `dlt resource`

    Returns:
        Any: TDltResourceImpl instance which may be loaded, iterated or combined with other resources into a pipeline.
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
        references=references,
        nested_hints=nested_hints,
        selected=selected,
        spec=spec,
        data_from=data_from,
        parallelized=parallelized,
        _impl_cls=_impl_cls,
        section=section,
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
    """Gets the source section name (as in SOURCES (section, name) tuple) from __source_name__ of the module `m` or from its name"""
    if m is None:
        return None
    if hasattr(m, "__source_name__"):
        return cast(str, m.__source_name__)
    return get_module_name(m)


def get_source_schema() -> Schema:
    """Should be executed from inside the function decorated with @dlt.resource

    Returns:
        Schema: The current writeable source schema
    """
    try:
        return Container()[SourceSchemaInjectableContext].schema
    except ContextDefaultCannotBeCreated:
        raise CurrentSourceSchemaNotAvailable()


def get_source() -> DltSource:
    """Should be executed from inside the function decorated with @dlt.resource

    Returns:
        DltSource: The current writable source object
    """
    try:
        return Container()[SourceInjectableContext].source
    except ContextDefaultCannotBeCreated:
        raise CurrentSourceNotAvailable()


TBoundItems = TypeVar("TBoundItems", bound=TDataItems)
TDeferred = Callable[[], TBoundItems]
TDeferredFunParams = ParamSpec("TDeferredFunParams")


def defer(
    f: Callable[TDeferredFunParams, TBoundItems],
) -> Callable[TDeferredFunParams, TDeferred[TBoundItems]]:
    @wraps(f)
    def _wrap(*args: Any, **kwargs: Any) -> TDeferred[TBoundItems]:
        def _curry() -> TBoundItems:
            return f(*args, **kwargs)

        return _curry

    # copy the signature from f to _wrap, but change the return type
    sig = inspect.signature(f)
    return_annotation = (
        Callable[[], sig.return_annotation]
        if sig.return_annotation is not inspect.Parameter.empty
        else None
    )
    if return_annotation:
        new_sig = sig.replace(return_annotation=return_annotation)
        setattr(_wrap, "__signature__", new_sig)

    return _wrap
