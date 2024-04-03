import dataclasses
from typing import Callable, Dict, Final, Optional, Type, Union

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
"""A callback which should return prepared string value the following arguments passed
`schema name`, `table name`, `load_id`, `file_id`, `extension` and `current_datetime`.
"""


@configspec
class FilesystemDestinationClientConfiguration(FilesystemConfiguration, DestinationClientStagingConfiguration):  # type: ignore[misc]
    destination_type: Final[str] = dataclasses.field(default="filesystem", init=False, repr=False, compare=False)  # type: ignore
    current_datetime: Optional[Union[DateTime, TCurrentDatetimeCallback]] = None
    datetime_format: Optional[TDatetimeFormat] = None
    extra_placeholders: Optional[Dict[str, Union[str, TLayoutParamCallback]]] = None

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return self.PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration]  # type: ignore[return-value]
