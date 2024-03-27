import dataclasses

from typing import Callable, Dict, Final, Type, Optional, TypeAlias, Union


from pendulum.datetime import DateTime
from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination.reference import (
    CredentialsConfiguration,
    DestinationClientStagingConfiguration,
)
from dlt.common.schema.schema import Schema
from dlt.common.storages import FilesystemConfiguration
from dlt.destinations.path_utils import PathParams, check_layout

TCurrentDatetimeCallback: TypeAlias = Callable[[], DateTime]
"""A callback which should return current datetime"""

TDatetimeFormatterCallback: TypeAlias = Callable[[DateTime], str]
"""A callback which is responsible to format datetime"""

TDatetimeFormat: TypeAlias = Union[str, TDatetimeFormatterCallback]
"""Datetime format or formatter callback"""

TLayoutParamCallback: TypeAlias = Callable[[Schema, DateTime], str]
"""A callback which should return prepared string value for layout parameter value"""


@configspec
class FilesystemDestinationClientConfiguration(
    FilesystemConfiguration, DestinationClientStagingConfiguration
):  # type: ignore[misc]
    destination_type: Final[str] = dataclasses.field(
        default="filesystem", init=False, repr=False, compare=False
    )  # type: ignore
    current_datetime: Optional[Union[DateTime, TCurrentDatetimeCallback]] = None
    datetime_format: Optional[TDatetimeFormat] = None
    layout_params: Optional[Dict[str, Union[str, TLayoutParamCallback]]] = None
    suffix: Optional[Union[str, Callable[[PathParams], str]]] = None

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return (
            self.PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration]
        )  # type: ignore[return-value]

    def on_resolved(self) -> None:
        # If current_datetime is a callable
        # then we need to inspect it's return type
        # if return type is not DateTime
        # then call it and check it's instance
        # if it is not DateTime then exit.
        if self.current_datetime is not None:
            if callable(self.current_datetime):
                result = self.current_datetime()
                if isinstance(result, DateTime):
                    self.current_datetime = result
                else:
                    raise RuntimeError(
                        "current_datetime was passed as callable but "
                        "didn't return any instance of pendulum.DateTime"
                    )


        # Validate layout and layout params
        check_layout(self.layout, self.layout_params)
        super().on_resolved()
