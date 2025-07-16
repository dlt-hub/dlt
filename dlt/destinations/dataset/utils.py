from typing import TYPE_CHECKING

if not TYPE_CHECKING:
    from dlt.common.warnings import DltDeprecationWarning

    DltDeprecationWarning(
        "Content from this module was moved to `dlt._dataset.utils`, which is internal.",
        since="1.15.0",
        expected_due="2.0.0",
    )

from dlt._dataset.utils import get_destination_client_initial_config, get_destination_clients
