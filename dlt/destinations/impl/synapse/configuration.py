from typing import Final, Any, List, Dict, Optional, ClassVar

from dlt.common import logger
from dlt.common.configuration import configspec
from dlt.common.schema.typing import TSchemaTables
from dlt.common.schema.utils import get_write_disposition

from dlt.destinations.impl.mssql.configuration import (
    MsSqlCredentials,
    MsSqlClientConfiguration,
)
from dlt.destinations.impl.mssql.configuration import MsSqlCredentials

from dlt.destinations.impl.synapse.synapse_adapter import TTableIndexType


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

    # While Synapse uses CLUSTERED COLUMNSTORE INDEX tables by default, we use
    # HEAP tables (no indexing) by default. HEAP is a more robust choice, because
    # columnstore tables do not support varchar(max), nvarchar(max), and varbinary(max).
    # https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-index
    default_table_index_type: Optional[TTableIndexType] = "heap"
    """
    Table index type that is used if no table index type is specified on the resource.
    This only affects data tables, dlt system tables ignore this setting and
    are always created as "heap" tables.
    """

    # Set to False by default because the PRIMARY KEY and UNIQUE constraints
    # are tricky in Synapse: they are NOT ENFORCED and can lead to innacurate
    # results if the user does not ensure all column values are unique.
    # https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-table-constraints
    create_indexes: bool = False
    """Whether `primary_key` and `unique` column hints are applied."""

    staging_use_msi: bool = False
    """Whether the managed identity of the Synapse workspace is used to authorize access to the staging Storage Account."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "default_table_index_type",
        "create_indexes",
        "staging_use_msi",
    ]
