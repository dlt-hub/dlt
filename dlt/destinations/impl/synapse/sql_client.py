from typing import ClassVar
from contextlib import suppress

from dlt.common.destination import DestinationCapabilitiesContext

from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient
from dlt.destinations.impl.mssql.configuration import MsSqlCredentials
from dlt.destinations.impl.synapse import capabilities
from dlt.destinations.impl.synapse.configuration import SynapseCredentials

from dlt.destinations.exceptions import DatabaseUndefinedRelation


class SynapseSqlClient(PyOdbcMsSqlClient):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def drop_tables(self, *tables: str) -> None:
        if not tables:
            return
        # Synapse does not support DROP TABLE IF EXISTS.
        # Workaround: use DROP TABLE and suppress non-existence errors.
        statements = [f"DROP TABLE {self.make_qualified_table_name(table)};" for table in tables]
        with suppress(DatabaseUndefinedRelation):
            self.execute_fragments(statements)

    def _drop_schema(self) -> None:
        # Synapse does not support DROP SCHEMA IF EXISTS.
        self.execute_sql("DROP SCHEMA %s;" % self.fully_qualified_dataset_name())
