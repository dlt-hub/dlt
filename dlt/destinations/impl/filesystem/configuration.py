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

TDatetimeFormatterCallback: TypeAlias = Callable[[DateTime], str]
TDatetimeFormat: TypeAlias = Union[str, TDatetimeFormatterCallback]
TLayoutParamCallback: TypeAlias = Callable[[Schema, DateTime], str]


@configspec
class FilesystemDestinationClientConfiguration(FilesystemConfiguration, DestinationClientStagingConfiguration):  # type: ignore[misc]
    destination_type: Final[str] = dataclasses.field(default="filesystem", init=False, repr=False, compare=False)  # type: ignore
    current_datetime: Optional[DateTime] = None
    datetime_format: Optional[TDatetimeFormat] = None
    layout_params: Optional[Dict[str, Union[str, TLayoutParamCallback]]] = None


    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return self.PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration]  # type: ignore[return-value]

    def on_resolved(self) -> None:
        import ipdb;ipdb.set_trace()
        super().on_resolved()
