import dataclasses
from typing import Optional, Final, Callable, Union
from typing_extensions import ParamSpec

from dlt.common.configuration import configspec
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import (
    DestinationClientConfiguration,
)
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
from dlt.common.destination import Destination

TDestinationCallable = Callable[[Union[TDataItems, str], TTableSchema], None]
TDestinationCallableParams = ParamSpec("TDestinationCallableParams")


@configspec
class CustomDestinationClientConfiguration(DestinationClientConfiguration):
    destination_type: Final[str] = dataclasses.field(default="destination", init=False, repr=False, compare=False)  # type: ignore
    destination_callable: Optional[Union[str, TDestinationCallable]] = None  # noqa: A003
    loader_file_format: TLoaderFileFormat = "typed-jsonl"
    batch_size: int = 10
    skip_dlt_columns_and_tables: bool = True
    max_table_nesting: int = 0
