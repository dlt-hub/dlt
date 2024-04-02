import dataclasses

from typing import Any, Callable, Dict, Final, Type, Optional, Union

from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination.reference import (
    CredentialsConfiguration,
    DestinationClientStagingConfiguration,
)
from dlt.common.storages import FilesystemConfiguration
from pendulum.datetime import DateTime
from typing_extensions import TypeAlias

TCurrentDatetimeCallback: TypeAlias = Callable[[], DateTime]
"""A callback which should return current datetime"""

TDatetimeFormat: TypeAlias = str
"""Datetime format or formatter callback"""

TLayoutParamCallback: TypeAlias = Callable[[str, str, str, str, str, DateTime], str]
"""A callback which should return prepared string value for layout parameter value
schema name, table name, load_id, file_id, extension and current_datetime will be passed
"""


@configspec
class FilesystemDestinationClientConfiguration(  # type: ignore[misc]
    FilesystemConfiguration, DestinationClientStagingConfiguration
):
    destination_type: Final[str] = dataclasses.field(  # type: ignore
        default="filesystem", init=False, repr=False, compare=False
    )
    current_datetime: Optional[Union[DateTime, TCurrentDatetimeCallback]] = None
    datetime_format: Optional[TDatetimeFormat] = None
    extra_params: Optional[Dict[str, Union[str, TLayoutParamCallback]]] = None

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return (
            self.PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration]  # type: ignore[return-value]
        )

    def on_resolved(self) -> None:
        """Resolve configuration for filesystem destination

        The following three variables will override the ones from configuration
        if supplied via `filesystem(...)` constructor, additionally
        when provided `extra_params` will be merged with the values from configuration
        however provided values will always override config.toml.

            * current_datetime,
            * datetime_format,
            * extra_params
        """
        if current_datetime := self.kwargs.get("current_datetime"):
            self.current_datetime = current_datetime

        datetime_format = self.kwargs.get("datetime_format", self.datetime_format)
        if datetime_format is not None:
            self.datetime_format = datetime_format

        extra_params_arg: Dict[str, Any] = self.kwargs.get("extra_params", {})
        if not self.extra_params and extra_params_arg:
            self.extra_params = {}

        if extra_params_arg:
            for key, val in extra_params_arg.items():
                self.extra_params[key] = val

        super().on_resolved()
