from typing import Any, Literal, Union, overload

from dlt.common.destination import TDestinationReferenceArg
from dlt.common.destination.typing import TDatasetType
from dlt.common.schema import Schema
from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException

from dlt.destinations.dataset.dataset import ReadableDBAPIDataset, ReadableIbisDataset

# NOTE: I expect that we'll merge all relations into one. and then we'll be able to get rid
#  of overload and dataset_type


@overload
def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
    dataset_type: Literal["ibis"] = "ibis",
) -> ReadableIbisDataset: ...


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
) -> ReadableIbisDataset: ...


def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
    dataset_type: TDatasetType = "auto",
) -> Any:
    if dataset_type == "ibis":
        logger.warning(
            "The dataset_type 'ibis' is deprecated and will be removed in an upcoming release."
            " Please use dataset_type 'default' instead. Ibis expressions are still fully"
            " supported, please refer to the dataset documentation at"
            " https://dlthub.com/docs/general-usage/dataset-access/dataset#modifying-queries-with-ibis-expressions"
            " for more information."
        )

    if dataset_type == "auto":
        logger.warning(
            "You are using the dataset with dataset_type set to 'auto'. which is the default value."
            " This will automatically select the dataset type based on wether ibis is available or"
            " not. We suggest to set the dataset_type explicitly to 'default' to get the correct"
            " typing for the dataset object and the relations and guard against the future removal"
            " of the ibis dataset type. Ibis relations are still fully supported with the default"
            " dataset type, please refer to the dataset documentation at"
            " https://dlthub.com/docs/general-usage/dataset-access/dataset#modifying-queries-with-ibis-expressions"
            " for more information and how to migrate."
        )

    # resolve dataset type
    if dataset_type in ("auto", "ibis"):
        try:
            from dlt.helpers.ibis import ibis

            dataset_type = "ibis"
        except MissingDependencyException:
            # if ibis is explicitly requested, reraise
            if dataset_type == "ibis":
                raise
            dataset_type = "default"

    if dataset_type == "ibis":
        return ReadableIbisDataset(destination, dataset_name, schema)
    elif dataset_type == "default":
        return ReadableDBAPIDataset(destination, dataset_name, schema)
