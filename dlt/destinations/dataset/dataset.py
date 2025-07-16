from typing import TYPE_CHECKING

if not TYPE_CHECKING:
    from dlt.common.warnings import DltDeprecationWarning

    DltDeprecationWarning(
        """Content from this module was moved to `dlt._dataset.dataset`, which is internal. \
        If you need to import `ReadableDBAPIDataset`, import `dlt.Dataset` instead.""",
        since="1.15",
        expected_due="2.0",
    )

from dlt._dataset.dataset import Dataset

ReadableDBAPIDataset = Dataset
