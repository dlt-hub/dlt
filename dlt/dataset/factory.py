from typing import Union, TYPE_CHECKING, Any

from dlt.common.destination import TDestinationReferenceArg
from dlt.common.schema import Schema

from dlt.dataset.dataset import Dataset

if TYPE_CHECKING:
    from dlt import SupportsDataset
else:
    SupportsDataset = Any


def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
) -> SupportsDataset:
    return Dataset(destination, dataset_name, schema)
