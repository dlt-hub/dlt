from typing import Any, Type
from dlt.destinations.impl.destination.factory import destination as _destination
from dlt.destinations.impl.destination.configuration import SinkClientConfiguration, TSinkCallable
from dlt.common.destination import TDestinationReferenceArg
from dlt.common.destination import TLoaderFileFormat


def destination(
    loader_file_format: TLoaderFileFormat = None,
    batch_size: int = 10,
    name: str = None,
    naming_convention: str = "direct",
    spec: Type[SinkClientConfiguration] = SinkClientConfiguration,
) -> Any:
    def decorator(destination_callable: TSinkCallable) -> TDestinationReferenceArg:
        # return destination instance
        return _destination(
            spec=spec,
            destination_callable=destination_callable,
            loader_file_format=loader_file_format,
            batch_size=batch_size,
            destination_name=name,
            naming_convention=naming_convention,
        )

    return decorator
