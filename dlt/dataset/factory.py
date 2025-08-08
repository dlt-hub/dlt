from typing import Union, TYPE_CHECKING, Any

from dlt.common.destination import TDestinationReferenceArg
from dlt.common.schema import Schema

from dlt.dataset.dataset import ReadableDBAPIDataset

if TYPE_CHECKING:
    from dlt import SupportsDataset
else:
    SupportsDataset = Any


def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
) -> SupportsDataset:
    return ReadableDBAPIDataset(destination, dataset_name, schema)
