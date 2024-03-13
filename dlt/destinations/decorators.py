import functools

from typing import Any, Type, Optional, Callable, Union
from typing_extensions import Concatenate

from functools import wraps

from dlt.destinations.impl.destination.factory import destination as _destination
from dlt.destinations.impl.destination.configuration import (
    TDestinationCallableParams,
    GenericDestinationClientConfiguration,
)
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import Destination
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema


def destination(
    *,
    loader_file_format: TLoaderFileFormat = None,
    batch_size: int = 10,
    name: str = None,
    naming_convention: str = "direct",
    spec: Type[GenericDestinationClientConfiguration] = GenericDestinationClientConfiguration,
) -> Callable[
    [Callable[Concatenate[Union[TDataItems, str], TTableSchema, TDestinationCallableParams], Any]],
    Callable[TDestinationCallableParams, _destination],
]:
    def decorator(
        destination_callable: Callable[
            Concatenate[Union[TDataItems, str], TTableSchema, TDestinationCallableParams], Any
        ]
    ) -> Callable[TDestinationCallableParams, _destination]:
        @wraps(destination_callable)
        def wrapper(
            *args: TDestinationCallableParams.args, **kwargs: TDestinationCallableParams.kwargs
        ) -> _destination:
            return _destination(
                spec=spec,
                destination_callable=destination_callable,
                loader_file_format=loader_file_format,
                batch_size=batch_size,
                destination_name=name,
                naming_convention=naming_convention,
                **kwargs,  # type: ignore
            )

        return wrapper

    return decorator
