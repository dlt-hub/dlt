"""SQL client for Fabric Warehouse - extends Synapse SQL client"""

import struct
from typing import TYPE_CHECKING, Any

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.impl.synapse.sql_client import SynapseSqlClient
from dlt.destinations.impl.mssql.sql_client import handle_datetimeoffset

if TYPE_CHECKING:
    from dlt.destinations.impl.fabric.configuration import FabricCredentials


SQL_COPT_SS_ACCESS_TOKEN = 1256


class FabricSqlClient(SynapseSqlClient):
    """SQL client for Microsoft Fabric Warehouse.

    Overrides `open_connection` to support passing a pre-fetched AAD bearer
    token via `attrs_before={SQL_COPT_SS_ACCESS_TOKEN: ...}` when the
    credentials object is in notebook-token mode.
    """

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: "FabricCredentials",
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(dataset_name, staging_dataset_name, credentials, capabilities)  # type: ignore[arg-type]
        self.credentials: "FabricCredentials" = credentials  # type: ignore[assignment]

    def open_connection(self) -> Any:
        """Open a pyodbc connection, passing an AAD bearer token when available."""
        import pyodbc

        token_str = self.credentials.get_access_token()
        if token_str is None:
            return super().open_connection()

        raw = token_str.encode("utf-16-le")
        token_struct = struct.pack(f"<I{len(raw)}s", len(raw), raw)
        self._conn = pyodbc.connect(
            self.credentials.to_odbc_dsn(),
            timeout=self.credentials.connect_timeout,
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
        )
        self._conn.add_output_converter(-155, handle_datetimeoffset)
        self._conn.autocommit = True
        return self._conn
