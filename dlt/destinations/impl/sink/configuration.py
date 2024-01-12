from typing import TYPE_CHECKING, Optional, Final, Callable, Union, Any

from dlt.common.configuration import configspec
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import (
    DestinationClientConfiguration,
    CredentialsConfiguration,
)
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema


TSinkCallable = Callable[[TDataItems, TTableSchema], None]


@configspec
class SinkClientCredentials(CredentialsConfiguration):
    callable_name: Optional[str] = None

    def parse_native_representation(self, native_value: Any) -> None:
        if callable(native_value):
            self.callable: TSinkCallable = native_value


@configspec
class SinkClientConfiguration(DestinationClientConfiguration):
    destination_type: Final[str] = "sink"  # type: ignore
    credentials: SinkClientCredentials = None
    loader_file_format: TLoaderFileFormat = "parquet"
    batch_size: int = 10

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            credentials: Union[SinkClientCredentials, TSinkCallable] = None,
        ) -> None: ...
