import typing as t
import inspect
from importlib import import_module
from types import ModuleType

from dlt.common import logger
from dlt.common.destination.capabilities import TLoaderParallelismStrategy
from dlt.common.destination.exceptions import DestinationException
from dlt.common.exceptions import TerminalValueError
from dlt.common.normalizers.naming.naming import NamingConvention
from dlt.common.reflection.exceptions import ReferenceImportError
from dlt.common.reflection.ref import ImportTrace, callable_typechecker, object_from_ref
from dlt.common.typing import AnyFun
from dlt.common.destination import Destination, DestinationCapabilitiesContext, TLoaderFileFormat
from dlt.common.configuration import known_sections, with_config, get_fun_spec
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.utils import get_callable_name, is_inner_callable

from dlt.destinations.impl.destination.configuration import (
    CustomDestinationClientConfiguration,
    dummy_custom_destination,
    TDestinationCallable,
)

if t.TYPE_CHECKING:
    from dlt.destinations.impl.destination.destination import DestinationClient


class UnknownCustomDestinationCallable(ReferenceImportError, DestinationException, KeyError):
    def __init__(
        self, ref: str, qualified_refs: t.Sequence[str], traces: t.Sequence[ImportTrace]
    ) -> None:
        self.ref = ref
        self.qualified_refs = qualified_refs
        super().__init__(traces=traces)

    def __str__(self) -> str:
        if "." in self.ref:
            msg = f"Custom destination callable {self.ref} could not be imported."

        if len(self.qualified_refs) == 1 and self.qualified_refs[0] == self.ref:
            pass
        else:
            msg += (
                " Following fully qualified refs were tried in the registry:\n\t%s\n"
                % "\n\t".join(self.qualified_refs)
            )
        if self.traces:
            msg += super().__str__()
        return msg


class destination(Destination[CustomDestinationClientConfiguration, "DestinationClient"]):
    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext.generic_capabilities("typed-jsonl")
        caps.supported_loader_file_formats = ["typed-jsonl", "parquet"]
        caps.supports_ddl_transactions = False
        caps.supports_transactions = False
        caps.naming_convention = "direct"
        caps.max_table_nesting = 0
        caps.max_parallel_load_jobs = 0
        caps.loader_parallelism_strategy = None
        return caps

    @property
    def spec(self) -> t.Type[CustomDestinationClientConfiguration]:
        """A spec of destination configuration resolved from the sink function signature"""
        return self._spec

    @property
    def client_class(self) -> t.Type["DestinationClient"]:
        from dlt.destinations.impl.destination.destination import DestinationClient

        return DestinationClient

    def __init__(
        self,
        destination_callable: t.Union[TDestinationCallable, str] = None,  # noqa: A003
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        loader_file_format: TLoaderFileFormat = None,
        batch_size: int = 10,
        naming_convention: str = "direct",
        spec: t.Type[CustomDestinationClientConfiguration] = None,
        **kwargs: t.Any,
    ) -> None:
        if spec and not issubclass(spec, CustomDestinationClientConfiguration):
            raise TerminalValueError(
                "A SPEC for a sink destination must use CustomDestinationClientConfiguration as a"
                " base."
            )
        # resolve callable
        if callable(destination_callable):
            pass
        elif destination_callable:
            imported_callable, trace = object_from_ref(destination_callable, callable_typechecker)
            if imported_callable is None:
                raise UnknownCustomDestinationCallable(
                    destination_callable, [destination_callable], [trace]
                )
            else:
                destination_callable = imported_callable
        # provide dummy callable for cases where no callable is provided
        # this is needed for cli commands to work
        if not destination_callable:
            logger.warning(
                "No destination callable provided, providing dummy callable which will fail on"
                " load."
            )
            destination_callable = dummy_custom_destination
        elif not callable(destination_callable):
            raise ConfigurationValueError("Resolved Sink destination callable is not a callable.")
        if not destination_name:
            destination_name = destination_callable.__name__

        # build destination spec
        destination_sections = (known_sections.DESTINATION, destination_name)
        conf_callable = with_config(
            destination_callable,
            spec=spec,
            sections=destination_sections,
            include_defaults=True,
            base=None if spec else CustomDestinationClientConfiguration,
        )

        # save destination in registry
        resolved_spec = t.cast(
            t.Type[CustomDestinationClientConfiguration], get_fun_spec(conf_callable)
        )
        # remember spec
        self._spec = resolved_spec or spec
        super().__init__(
            destination_name=destination_name,
            environment=environment,
            # NOTE: `loader_file_format` is not a field in the caps so we had to hack the base class to allow this
            loader_file_format=loader_file_format,
            batch_size=batch_size,
            naming_convention=naming_convention,
            destination_callable=conf_callable,
            **kwargs,
        )

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: CustomDestinationClientConfiguration,
        naming: t.Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        caps = super().adjust_capabilities(caps, config, naming)
        caps.preferred_loader_file_format = config.loader_file_format
        return caps


destination.register()
