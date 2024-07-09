import typing as t
import inspect
from importlib import import_module
from types import ModuleType

from dlt.common import logger
from dlt.common.destination.capabilities import TLoaderParallelismStrategy
from dlt.common.exceptions import TerminalValueError
from dlt.common.normalizers.naming.naming import NamingConvention
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


class DestinationInfo(t.NamedTuple):
    """Runtime information on a discovered destination"""

    SPEC: t.Type[CustomDestinationClientConfiguration]
    f: AnyFun
    module: ModuleType


_DESTINATIONS: t.Dict[str, DestinationInfo] = {}
"""A registry of all the decorated destinations"""


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
            if "." not in destination_callable:
                raise ValueError("str destination reference must be of format 'module.function'")
            module_path, attr_name = destination_callable.rsplit(".", 1)
            try:
                dest_module = import_module(module_path)
            except ModuleNotFoundError as e:
                raise ConfigurationValueError(
                    f"Could not find callable module at {module_path}"
                ) from e
            try:
                destination_callable = getattr(dest_module, attr_name)
            except AttributeError as e:
                raise ConfigurationValueError(
                    f"Could not find callable function at {destination_callable}"
                ) from e

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

        # resolve destination name
        if destination_name is None:
            destination_name = get_callable_name(destination_callable)
        func_module = inspect.getmodule(destination_callable)

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
        # register only standalone destinations, no inner
        if not is_inner_callable(destination_callable):
            _DESTINATIONS[destination_callable.__qualname__] = DestinationInfo(
                resolved_spec, destination_callable, func_module
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
