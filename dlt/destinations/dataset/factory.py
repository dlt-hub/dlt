from typing import Union


from dlt.common.destination import AnyDestination
from dlt.common.destination.reference import (
    SupportsReadableDataset,
    TDatasetType,
    TDestinationReferenceArg,
)

from dlt.common.schema import Schema

from dlt.destinations.dataset.dataset import ReadableDBAPIDataset


def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
    dataset_type: TDatasetType = "auto",
) -> SupportsReadableDataset:
    return ReadableDBAPIDataset(destination, dataset_name, schema, dataset_type)
