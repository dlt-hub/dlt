import typing as t
import inspect
from importlib import import_module

from types import ModuleType
from dlt.common.typing import AnyFun

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.configuration import known_sections, with_config, get_fun_spec
from dlt.common.configuration.exceptions import ConfigurationValueError

from dlt.destinations.impl.destination.configuration import (
    GenericDestinationClientConfiguration,
    TDestinationCallable,
)
from dlt.destinations.impl.destination import capabilities
from dlt.common.data_writers import TLoaderFileFormat
from dlt.common.utils import get_callable_name

if t.TYPE_CHECKING:
    from dlt.destinations.impl.destination.destination import DestinationClient


class DestinationInfo(t.NamedTuple):
    """Runtime information on a discovered destination"""

    SPEC: t.Type[GenericDestinationClientConfiguration]
    f: AnyFun
    module: ModuleType


_DESTINATIONS: t.Dict[str, DestinationInfo] = {}
"""A registry of all the decorated destinations"""


class destination(Destination[GenericDestinationClientConfiguration, "DestinationClient"]):
    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities(
            self.config_params.get("loader_file_format", "puae-jsonl"),
            self.config_params.get("naming_convention", "direct"),
        )

    @property
    def spec(self) -> t.Type[GenericDestinationClientConfiguration]:
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
        spec: t.Type[GenericDestinationClientConfiguration] = GenericDestinationClientConfiguration,
        **kwargs: t.Any,
    ) -> None:
        # resolve callable
        if callable(destination_callable):
            pass
        elif destination_callable:
            try:
                module_path, attr_name = destination_callable.rsplit(".", 1)
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

        if not callable(destination_callable):
            raise ConfigurationValueError("Resolved Sink destination callable is not a callable.")

        # resolve destination name
        if destination_name is None:
            destination_name = get_callable_name(destination_callable)
        func_module = inspect.getmodule(destination_callable)

        # build destination spec
        destination_sections = (known_sections.DESTINATION, destination_name)
        conf_callable = with_config(
            destination_callable,
            sections=destination_sections,
            include_defaults=True,
            base=spec,
        )

        # save destination in registry
        resolved_spec = t.cast(
            t.Type[GenericDestinationClientConfiguration], get_fun_spec(conf_callable)
        )
        _DESTINATIONS[callable.__qualname__] = DestinationInfo(resolved_spec, callable, func_module)

        # remember spec
        self._spec = resolved_spec or spec
        super().__init__(
            destination_name=destination_name,
            environment=environment,
            loader_file_format=loader_file_format,
            batch_size=batch_size,
            naming_convention=naming_convention,
            destination_callable=conf_callable,
            **kwargs,
        )
