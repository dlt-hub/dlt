from typing import Final, Any, List, Dict, Optional, ClassVar

from dlt.common import logger
from dlt.common.configuration import configspec
from dlt.common.schema.typing import TTableIndexType, TSchemaTables
from dlt.common.schema.utils import get_write_disposition

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
    create_indexes: Optional[bool] = False
    """Whether `primary_key` and `unique` column hints are applied."""

    # Concurrency is disabled by overriding the configured number of workers to 1 at runtime.
    auto_disable_concurrency: Optional[bool] = True
    """Whether concurrency is automatically disabled in cases where it might cause issues."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "default_table_index_type",
        "create_indexes",
        "auto_disable_concurrency",
    ]

    def get_load_workers(self, tables: TSchemaTables, workers: int) -> int:
        """Returns the adjusted number of load workers to prevent concurrency issues."""

        write_dispositions = [get_write_disposition(tables, table_name) for table_name in tables]
        n_replace_dispositions = len([d for d in write_dispositions if d == "replace"])
        if (
            n_replace_dispositions > 1
            and self.replace_strategy == "staging-optimized"
            and workers > 1
        ):
            warning_msg_shared = (
                'Data is being loaded into Synapse with write disposition "replace"'
                ' and replace strategy "staging-optimized", while the number of'
                f" load workers ({workers}) > 1. This configuration is problematic"
                " in some cases, because Synapse does not always handle concurrency well"
                " with the CTAS queries that are used behind the scenes to implement"
                ' the "staging-optimized" strategy.'
            )
            if self.auto_disable_concurrency:
                logger.warning(
                    warning_msg_shared
                    + " The number of load workers will be automatically adjusted"
                    " and set to 1 to eliminate concurrency and prevent potential"
                    " issues. If you don't want this to happen, set the"
                    " DESTINATION__SYNAPSE__AUTO_DISABLE_CONCURRENCY environment"
                    ' variable to "false", or add the following to your config TOML:'
                    "\n\n[destination.synapse]\nauto_disable_concurrency = false\n"
                )
                workers = 1  # adjust workers
            else:
                logger.warning(
                    warning_msg_shared
                    + " If you experience your pipeline gets stuck and doesn't finish,"
                    " try reducing the number of load workers by exporting the LOAD__WORKERS"
                    " environment variable or by setting it in your config TOML:"
                    "\n\n[load]\nworkers = 1 #  a value of 1 disables all concurrency,"
                    " but perhaps a higher value also works\n\n"
                    "Alternatively, you can set the DESTINATION__SYNAPSE__AUTO_DISABLE_CONCURRENCY"
                    ' environment variable to "true", or add the following to your config TOML'
                    " to automatically disable concurrency where needed:"
                    "\n\n[destination.synapse]\nauto_disable_concurrency = true\n"
                )
        return workers
