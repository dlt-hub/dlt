"""The module `dlt.destinations.dataset` is deprecated. Use in `dlt.dataset`

Changes were made backwards-compatible:
- `dlt.destinations.dataset.dataset.ReadableDBAPIDataset` -> `dlt.Dataset`
- `dlt.destinations.dataset.relation.ReadableDBAPIRelation` -> `dlt.Relation`
- `dlt.destinations.dataset::dataset()` -> `dlt.dataset()`
"""

from typing import TYPE_CHECKING

# NOTE this warning is only trigger when type-checking.
# Move it outside the type checking block when closer to deprecation date.
if not TYPE_CHECKING:
    from dlt.common.warnings import DltDeprecationWarning

    DltDeprecationWarning(
        "Content from this module was moved to `dlt._dataset`, which is internal.",
        since="1.16.0",
        expected_due="2.0.0",
    )

from dlt.dataset.dataset import dataset
from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
from dlt.destinations.dataset.relation import ReadableDBAPIRelation

from dlt.destinations.dataset.utils import (
    get_destination_clients,
    get_destination_client_initial_config,
)


__all__ = (
    "dataset",
    "ReadableDBAPIDataset",
    "ReadableDBAPIRelation",
    "get_destination_client_initial_config",
    "get_destination_clients",
)
