from typing import Any, Callable
from dlt.destinations.impl.sink.factory import sink as _sink
from dlt.destinations.impl.sink.configuration import SinkClientConfiguration, TSinkCallable
from dlt.common.destination import TDestinationReferenceArg


def sink() -> Any:
    def decorator(f: TSinkCallable) -> TDestinationReferenceArg:
        return _sink(credentials=f)

    return decorator
