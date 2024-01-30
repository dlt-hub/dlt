from typing import Any, Callable
from dlt.destinations.impl.sink.factory import sink as _sink
from dlt.destinations.impl.sink.configuration import SinkClientConfiguration, TSinkCallable
from dlt.common.destination import TDestinationReferenceArg
from dlt.common.destination import TLoaderFileFormat
from dlt.common.utils import get_callable_name


def sink(
    loader_file_format: TLoaderFileFormat = None, batch_size: int = 10, name: str = None
) -> Any:
    def decorator(f: TSinkCallable) -> TDestinationReferenceArg:
        nonlocal name
        if name is None:
            name = get_callable_name(f)
        return _sink(
            credentials=f, loader_file_format=loader_file_format, batch_size=batch_size, name=name
        )

    return decorator
