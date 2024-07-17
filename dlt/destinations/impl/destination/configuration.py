import dataclasses
from typing import Optional, Final, Callable, Union, Any
from typing_extensions import ParamSpec

from dlt.common.configuration import configspec, ConfigurationValueError
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import (
    DestinationClientConfiguration,
)
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema

TDestinationCallable = Callable[[Union[TDataItems, str], TTableSchema], None]
TDestinationCallableParams = ParamSpec("TDestinationCallableParams")


def dummy_custom_destination(*args: Any, **kwargs: Any) -> None:
    pass


@configspec
class CustomDestinationClientConfiguration(DestinationClientConfiguration):
    destination_type: Final[str] = dataclasses.field(default="destination", init=False, repr=False, compare=False)  # type: ignore
    destination_callable: Optional[Union[str, TDestinationCallable]] = None  # noqa: A003
    loader_file_format: TLoaderFileFormat = "typed-jsonl"
    batch_size: int = 10
    skip_dlt_columns_and_tables: bool = True
    max_table_nesting: Optional[int] = 0

    def ensure_callable(self) -> None:
        """Makes sure that valid callable was provided"""
        # TODO: this surely can be done with `on_resolved`
        if (
            self.destination_callable is None
            or self.destination_callable is dummy_custom_destination
        ):
            raise ConfigurationValueError(
                f"A valid callable was not provided to {self.__class__.__name__}. Did you decorate"
                " a function @dlt.destination correctly?"
            )
