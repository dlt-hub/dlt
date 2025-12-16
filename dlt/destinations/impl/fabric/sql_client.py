"""SQL client for Fabric Warehouse - extends Synapse SQL client"""

from dlt.destinations.impl.synapse.sql_client import SynapseSqlClient


class FabricSqlClient(SynapseSqlClient):
    """SQL client for Microsoft Fabric Warehouse

    Inherits all behavior from Synapse since Fabric Warehouse is built on Synapse technology.
    """

    pass
