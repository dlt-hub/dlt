from abc import ABC, abstractmethod
from importlib import import_module
from typing import (
    Callable,
    Dict,
    List,
    Any,
    Generic,
    Tuple,
    cast,
    overload,
)
from typing_extensions import Self, TypeVar
from typing import Dict, Type, ClassVar

from dlt.common import logger
from dlt.common.configuration.specs import BaseConfiguration, known_sections

from dlt.common.runtime.run_context import get_plugin_modules
from dlt.common.schema import Schema
from dlt.common.schema.typing import TSchemaContract
from dlt.common.typing import ParamSpec
from dlt.common.exceptions import MissingDependencyException
from dlt.common.warnings import Dlt04DeprecationWarning, deprecated

from dlt.extract.source import DltSource
from dlt.extract.exceptions import UnknownSourceReference

TDltSourceImpl = TypeVar("TDltSourceImpl", bound=DltSource, default=DltSource)
TSourceFunParams = ParamSpec("TSourceFunParams")


class SourceFactory(ABC, Generic[TSourceFunParams, TDltSourceImpl]):
    def __call__(
        self, *args: TSourceFunParams.args, **kwargs: TSourceFunParams.kwargs
    ) -> TDltSourceImpl:
        """Makes dlt source"""
        pass

    # TODO: make factory to expose SourceReference with actual spec, name and section
    # model after Destination, which also needs to be broken down into reference and factory
    ref: "SourceReference"

    @abstractmethod
    def clone(
        self,
        *,
        name: str = None,
        section: str = None,
        max_table_nesting: int = None,
        root_key: bool = False,
        schema: Schema = None,
        schema_contract: TSchemaContract = None,
        spec: Type[BaseConfiguration] = None,
        parallelized: bool = None,
        _impl_cls: Type[TDltSourceImpl] = DltSource,  # type: ignore[assignment]
    ) -> Self:
        """Overrides default decorator arguments that will be used to when DltSource instance and returns modified clone."""

    with_args = deprecated("Please use clone method instead", category=Dlt04DeprecationWarning)(
        clone
    )


AnySourceFactory = SourceFactory[Any, DltSource]


class SourceReference:
    SOURCES: ClassVar[Dict[str, "SourceReference"]] = {}
    """A registry of all the decorated sources and resources discovered when importing modules"""

    ref: str
    """A fully qualified reference: __module__.__name__ to the factory instance"""
    SPEC: Type[BaseConfiguration]
    factory: AnySourceFactory
    section: str
    name: str

    def __init__(
        self,
        ref: str,
        SPEC: Type[BaseConfiguration],
        factory: AnySourceFactory,
        section: str,
        name: str,
    ) -> None:
        self.ref = ref
        self.SPEC = SPEC
        self.factory = factory
        self.section = section
        self.name = name

    @classmethod
    def expand_shorthand_ref(cls, ref: str) -> List[str]:
        """Converts ref into fully qualified form, return one or more alternatives for shorthand notations.
        Run context is injected if needed. Following formats are recognized
        - section.name
        - name
        """
        ref_split = ref.split(".")
        ref_parts = len(ref_split)
        if ref_parts < 3:
            # expand with known prefixes
            refs = []
            for ref_prefix in get_plugin_modules():
                if ref_prefix:
                    ref_prefix = f"{ref_prefix}.{known_sections.SOURCES}"
                else:
                    ref_prefix = f"{known_sections.SOURCES}"
                # expand shorthand notation
                if ref_parts == 1:
                    refs.append(f"{ref_prefix}.{ref}.{ref}")
                elif ref_parts == 2:
                    # for ref with two parts two options are possible
                    refs.append(f"{ref_prefix}.{ref}")
            return refs
        return []

    @classmethod
    def register(cls, ref_obj: "SourceReference") -> None:
        ref = ref_obj.ref
        if ref in cls.SOURCES:
            logger.debug(f"A source with ref {ref} is already registered and will be overwritten")
        cls.SOURCES[ref] = ref_obj

    @overload
    @classmethod
    def find(cls, ref: str) -> AnySourceFactory: ...

    @overload
    @classmethod
    def find(
        cls,
        ref: str,
        /,
        _impl_sig: None = ...,
        _impl_cls: Type[TDltSourceImpl] = None,
    ) -> SourceFactory[Any, TDltSourceImpl]: ...

    @overload
    @classmethod
    def find(
        cls,
        ref: str,
        /,
        _impl_sig: Callable[TSourceFunParams, Any] = None,
        _impl_cls: Type[TDltSourceImpl] = None,
    ) -> SourceFactory[TSourceFunParams, TDltSourceImpl]: ...

    @classmethod
    def find(
        cls,
        ref: str,
        _impl_sig: Callable[TSourceFunParams, Any] = None,
        _impl_cls: Type[TDltSourceImpl] = None,
    ) -> Any:
        """Returns source factory from reference `ref`. Looks into registry or tries auto-import

        Expands shorthand notation into section.name eg. "sql_database" is expanded into
            "dlt.sources.sql_database.sql_database".
        """
        refs = cls.expand_shorthand_ref(ref)
        if ref not in refs:
            refs = [ref] + refs

        for ref_ in refs:
            if wrapper := cls.SOURCES.get(ref_):
                return wrapper.factory

        # try to import module
        for possible_type in refs:
            try:
                if "." not in possible_type:
                    continue
                # will expand type to import built in types
                module_path, attr_name = possible_type.rsplit(".", 1)
                dest_module = import_module(module_path)
                factory = cast(AnySourceFactory, getattr(dest_module, attr_name))
                # standalone resource will be implemented as decorated function with the factory attached
                if hasattr(factory, "_factory"):
                    factory = factory._factory
                # make sure it is factory interface (we could check Protocol as well)
                if not hasattr(factory, "clone"):
                    raise ValueError(f"{attr_name} in {module_path} is of type {type(factory)}")
                return factory
            except MissingDependencyException:
                raise
            except ModuleNotFoundError:
                # raise regular exception later
                pass
            except Exception as e:
                raise UnknownSourceReference([ref]) from e

        raise UnknownSourceReference(refs or [ref])

    @classmethod
    def from_reference(
        cls,
        ref: str,
        /,
        name: str = None,
        section: str = None,
        max_table_nesting: int = None,
        root_key: bool = False,
        schema: Schema = None,
        schema_contract: TSchemaContract = None,
        spec: Type[BaseConfiguration] = None,
        parallelized: bool = None,
        _impl_cls: Type[TDltSourceImpl] = None,
        source_args: Tuple[Any, ...] = None,
        source_kwargs: Dict[str, Any] = None,
    ) -> TDltSourceImpl:
        """Find registered source factory or imports it, then instantiates the DltSource using
        passed args and kwargs.
        Passes additional arguments to `clone` of source factory
        """
        source_args = source_args or ()
        source_kwargs = source_kwargs or {}
        return cls.find(ref, _impl_cls=_impl_cls).clone(
            name=name,
            section=section,
            max_table_nesting=max_table_nesting,
            root_key=root_key,
            schema=schema,
            schema_contract=schema_contract,
            spec=spec,
            parallelized=parallelized,
            _impl_cls=_impl_cls,
        )(*source_args, **source_kwargs)
