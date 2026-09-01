import dataclasses
from dlt import version
from typing import Final, List, Optional, ClassVar

from dlt.common.configuration import configspec
from dlt.common.destination.client import DestinationClientConfiguration

from dlt.destinations.impl.mssql.configuration import (
    MsSqlCredentials,
    MsSqlClientConfiguration,
)

from dlt.destinations.impl.synapse.synapse_adapter import TTableIndexType


@configspec(init=False)
class SynapseCredentials(MsSqlCredentials):
    drivername: Final[str] = dataclasses.field(default="synapse", init=False, repr=False, compare=False)  # type: ignore


@configspec
class SynapseClientConfiguration(MsSqlClientConfiguration):
    destination_type: Final[str] = dataclasses.field(default="synapse", init=False, repr=False, compare=False)  # type: ignore
    credentials: SynapseCredentials = None

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
    # are tricky in Synapse: they are NOT ENFORCED and can lead to inaccurate
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

    def data_location(self) -> str:
        """Returns host:port and the database. Synapse has no cross-database joins, unlike mssql."""
        host = super().data_location()
        if not self.credentials.database:
            self._no_data_location("the configuration has no database")
        return f"{host}/{self.credentials.database}"
