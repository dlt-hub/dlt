from typing import Union, TYPE_CHECKING, Any

from dlt.common.destination import TDestinationReferenceArg
from dlt.common.schema import Schema
from dlt.common import logger

from dlt.destinations.dataset.dataset import ReadableDBAPIDataset

if TYPE_CHECKING:
    from dlt import Dataset
else:
    Dataset = Any


def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
) -> Dataset:
    return ReadableDBAPIDataset(destination, dataset_name, schema)
