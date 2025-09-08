from typing import Any, Literal, Union, overload

from dlt.common.destination import TDestinationReferenceArg
from dlt.common.destination.typing import TDatasetType
from dlt.common.schema import Schema

from dlt.destinations.dataset.dataset import ReadableDBAPIDataset

# NOTE: I expect that we'll merge all relations into one. and then we'll be able to get rid
#  of overload and dataset_type


@overload
def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
    dataset_type: Literal["ibis"] = "ibis",
) -> ReadableDBAPIDataset: ...


@overload
def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
    dataset_type: Literal["default"] = "default",
) -> ReadableDBAPIDataset: ...


@overload
def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
    dataset_type: TDatasetType = "auto",
) -> ReadableDBAPIDataset: ...


def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
    dataset_type: TDatasetType = "auto",
) -> Any:
    return ReadableDBAPIDataset(destination, dataset_name, schema, dataset_type)
