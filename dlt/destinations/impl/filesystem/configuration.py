import dataclasses

from typing import Any, Callable, Dict, Final, Type, Optional, TypeAlias, Union


from pendulum.datetime import DateTime
from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination.reference import (
    CredentialsConfiguration,
    DestinationClientStagingConfiguration,
)
from dlt.common.storages import FilesystemConfiguration

TCurrentDatetimeCallback: TypeAlias = Callable[[], DateTime]
"""A callback which should return current datetime"""

TDatetimeFormatterCallback: TypeAlias = Callable[[DateTime], str]
"""A callback which is responsible to format datetime"""

TDatetimeFormat: TypeAlias = Union[str, TDatetimeFormatterCallback]
"""Datetime format or formatter callback"""

TLayoutParamCallback: TypeAlias = Callable[[str, str, str, str, DateTime], str]
"""A callback which should return prepared string value for layout parameter value
schema name, table name, load_id, file_id and current_datetime will be passed
"""


@configspec
class FilesystemDestinationClientConfiguration(
    FilesystemConfiguration, DestinationClientStagingConfiguration
):  # type: ignore[misc]
    destination_type: Final[str] = dataclasses.field(
        default="filesystem", init=False, repr=False, compare=False
    )  # type: ignore
    current_datetime: Optional[Union[DateTime, TCurrentDatetimeCallback]] = None
    datetime_format: Optional[TDatetimeFormat] = None
    extra_params: Optional[Dict[str, Union[str, TLayoutParamCallback]]] = None

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return (
            self.PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration]
        )  # type: ignore[return-value]

    def on_resolved(self) -> None:
        """Resolve configuration for filesystem destination

        The following three variables will override the ones
        if supplied via filesystem(...) constructor, additionally
        we merge extra_params if provided and when provided via
        the constructor it will take the priority over config.toml values

            * current_datetime,
            * datetime_format,
            * extra_params
        """
        # If current_datetime is a callable
        # then we need to inspect it's return type
        # if return type is not DateTime
        # then call it and check it's instance
        # if it is not DateTime then exit.
        current_datetime = self.kwargs.get("current_datetime", self.current_datetime)
        if current_datetime is not None:
            if callable(current_datetime):
                result = current_datetime()
                if isinstance(result, DateTime):
                    self.current_datetime = result
                else:
                    raise RuntimeError(
                        "current_datetime was passed as callable but "
                        "didn't return any instance of pendulum.DateTime"
                    )

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
