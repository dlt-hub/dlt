from typing import TYPE_CHECKING, Optional, Final, Callable, Union, Any
from typing_extensions import ParamSpec

from dlt.common.configuration import configspec
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import (
    DestinationClientConfiguration,
    CredentialsConfiguration,
)
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
from dlt.common.destination import Destination

TDestinationCallable = Callable[[Union[TDataItems, str], TTableSchema], None]
TDestinationCallableParams = ParamSpec("TDestinationCallableParams")


@configspec
class SinkClientConfiguration(DestinationClientConfiguration):
    destination_type: Final[str] = "sink"  # type: ignore
    destination_callable: Optional[Union[str, TDestinationCallable]] = None  # noqa: A003
    loader_file_format: TLoaderFileFormat = "puae-jsonl"
    batch_size: int = 10

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            loader_file_format: TLoaderFileFormat = "puae-jsonl",
            batch_size: int = 10,
            destination_callable: Union[TDestinationCallable, str] = None,
        ) -> None: ...
