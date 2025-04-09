from typing import Any, Literal, Union, overload

from dlt.common.destination import TDestinationReferenceArg
from dlt.common.destination.typing import TDatasetType
from dlt.common.schema import Schema

from dlt.destinations.dataset.dataset import ReadableDBAPIDataset, ReadableDBAPIRelation
from dlt.destinations.dataset.ibis_relation import ReadableIbisRelation


@overload
def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
    dataset_type: Literal["ibis"] = "ibis",
) -> ReadableDBAPIDataset[ReadableIbisRelation]: ...


@overload
def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
    dataset_type: Literal["default"] = "default",
) -> ReadableDBAPIDataset[ReadableDBAPIRelation]: ...


@overload
def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
    dataset_type: TDatasetType = "auto",
) -> ReadableDBAPIDataset[ReadableIbisRelation]: ...


def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
    dataset_type: TDatasetType = "auto",
) -> Any:
    return ReadableDBAPIDataset[ReadableIbisRelation](
        destination, dataset_name, schema, dataset_type
    )
