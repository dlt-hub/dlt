from typing import Final, Any, List, Dict, Optional, ClassVar

from dlt.common.configuration import configspec

from dlt.destinations.impl.mssql.configuration import (
    MsSqlCredentials,
    MsSqlClientConfiguration,
)
from dlt.destinations.impl.mssql.configuration import MsSqlCredentials


@configspec
class SynapseCredentials(MsSqlCredentials):
    drivername: Final[str] = "synapse"  # type: ignore

    # LongAsMax keyword got introduced in ODBC Driver 18 for SQL Server.
    SUPPORTED_DRIVERS: ClassVar[List[str]] = ["ODBC Driver 18 for SQL Server"]

    def _get_odbc_dsn_dict(self) -> Dict[str, Any]:
        params = super()._get_odbc_dsn_dict()
        # Long types (text, ntext, image) are not supported on Synapse.
        # Convert to max types using LongAsMax keyword.
        # https://stackoverflow.com/a/57926224
        params["LONGASMAX"] = "yes"
        return params


@configspec
class SynapseClientConfiguration(MsSqlClientConfiguration):
    destination_type: Final[str] = "synapse"  # type: ignore
    credentials: SynapseCredentials

    # Determines if `primary_key` and `unique` column hints are applied.
    # Set to False by default because the PRIMARY KEY and UNIQUE constraints
    # are tricky in Synapse: they are NOT ENFORCED and can lead to innacurate
    # results if the user does not ensure all column values are unique.
    # https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-table-constraints
    create_indexes: bool = False
