"""SQL client for Fabric Warehouse - extends Synapse SQL client"""

from typing import TYPE_CHECKING

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.impl.synapse.sql_client import SynapseSqlClient

if TYPE_CHECKING:
    from dlt.destinations.impl.fabric.configuration import FabricCredentials


class FabricSqlClient(SynapseSqlClient):
    """SQL client for Microsoft Fabric Warehouse

    Inherits all behavior from Synapse since Fabric Warehouse is built on Synapse technology.
    """

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: "FabricCredentials",
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        # FabricCredentials has all required attributes: database, to_odbc_dsn(), connect_timeout
        super().__init__(dataset_name, staging_dataset_name, credentials, capabilities)  # type: ignore[arg-type]
        self.credentials: "FabricCredentials" = credentials  # type: ignore[assignment]
