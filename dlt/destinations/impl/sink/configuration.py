from typing import TYPE_CHECKING, Optional, Final, Callable, Union, Any
from importlib import import_module

from dlt.common.configuration import configspec
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import (
    DestinationClientConfiguration,
    CredentialsConfiguration,
)
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
from dlt.common.configuration.exceptions import ConfigurationValueError


TSinkCallable = Callable[[Union[TDataItems, str], TTableSchema], None]


@configspec
class SinkClientCredentials(CredentialsConfiguration):
    callable: Optional[str] = None  # noqa: A003
    # name provides namespace for callable state saving
    name: Optional[str] = None

    def parse_native_representation(self, native_value: Any) -> None:
        # a callable was passed in
        if callable(native_value):
            self.resolved_callable: TSinkCallable = native_value
        # a path to a callable was passed in
        if isinstance(native_value, str):
            self.callable = native_value

    def to_native_representation(self) -> Any:
        return self.resolved_callable

    def on_resolved(self) -> None:
        if self.callable:
            try:
                module_path, attr_name = self.callable.rsplit(".", 1)
                dest_module = import_module(module_path)
            except ModuleNotFoundError as e:
                raise ConfigurationValueError(
                    f"Could not find callable module at {module_path}"
                ) from e
            try:
                self.resolved_callable = getattr(dest_module, attr_name)
            except AttributeError as e:
                raise ConfigurationValueError(
                    f"Could not find callable function at {self.callable}"
                ) from e

        if not hasattr(self, "resolved_callable"):
            raise ConfigurationValueError("Please specify callable for sink destination.")

        if not callable(self.resolved_callable):
            raise ConfigurationValueError("Resolved Sink destination callable is not a callable.")

        if not self.name:
            self.name = self.resolved_callable.__name__


@configspec
class SinkClientConfiguration(DestinationClientConfiguration):
    destination_type: Final[str] = "sink"  # type: ignore
    credentials: SinkClientCredentials = None
    loader_file_format: TLoaderFileFormat = "puae-jsonl"
    batch_size: int = 10

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            credentials: Union[SinkClientCredentials, TSinkCallable, str] = None,
            loader_file_format: TLoaderFileFormat = "puae-jsonl",
            batch_size: int = 10,
        ) -> None: ...
