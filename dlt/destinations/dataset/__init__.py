from typing import TYPE_CHECKING

if not TYPE_CHECKING:
    from dlt.common.warnings import DltDeprecationWarning

    DltDeprecationWarning(
        "Content from this module was moved to `dlt._dataset`, which is internal.",
        since="1.15",
        expected_due="2.0",
    )

from dlt.destinations.dataset.factory import dataset
from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
from dlt.destinations.dataset.relation import ReadableDBAPIRelation

from dlt.destinations.dataset.utils import (
    get_destination_clients,
    get_destination_client_initial_config,
)


__all__ = [
    "dataset",
    "ReadableDBAPIDataset",
    "ReadableDBAPIRelation",
    "get_destination_client_initial_config",
    "get_destination_clients",
]
