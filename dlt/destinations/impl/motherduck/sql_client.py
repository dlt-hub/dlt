from typing import ClassVar, Collection, List, Optional, cast

from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.motherduck.configuration import MotherDuckCredentials
from dlt.destinations.sql_client import (
    TAttachStatement,
    TAttachType,
    WithAttach,
    attach_statement,
)


class MotherDuckSqlClient(DuckDbSqlClient):
    attach_type: ClassVar[TAttachType] = "motherduck"
    """Separate from `duckdb` because these statements set a token before the connection is
    initialized and name a catalog alias, neither of which a MotherDuck connection accepts."""

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: MotherDuckCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(dataset_name, staging_dataset_name, credentials, capabilities)
        self.database_name = credentials.database

    def attach_statements(
        self, *, alias: str, tables: Optional[Collection[str]] = None
    ) -> List[TAttachStatement]:
        # the whole database is attached, `tables` cannot narrow it
        q_alias = self.escape_column_name(alias)
        attach = f"ATTACH IF NOT EXISTS 'md:{self.database_name}' AS {q_alias}"
        token = cast(MotherDuckCredentials, self.credentials).password
        if token:
            # `SET motherduck_token` does not exist until the extension is loaded, and the token
            # must be set before ATTACH. LOAD autoinstalls, so no INSTALL is needed. Only the
            # token line is secret
            statements = [
                attach_statement("LOAD motherduck"),
                attach_statement(
                    f"SET motherduck_token='{token}'", secret=True, key=f"{alias}:token"
                ),
                attach_statement(attach),
            ]
        else:
            statements = [attach_statement(attach)]
        return statements

    def catalog_name(self, quote: bool = True, casefold: bool = True) -> Optional[str]:
        if casefold:
            database_name = self.capabilities.casefold_identifier(self.database_name)
        else:
            database_name = self.database_name
        if quote:
            database_name = self.capabilities.escape_identifier(database_name)
        return database_name
