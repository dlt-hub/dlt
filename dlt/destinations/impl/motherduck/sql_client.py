from typing import ClassVar, Optional, Tuple, cast

from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.motherduck.configuration import MotherDuckCredentials
from dlt.destinations.sql_client import TAttachInfo, TAttachType, attach_statement


class MotherDuckSqlClient(DuckDbSqlClient):
    attach_type: ClassVar[TAttachType] = "motherduck"
    # a MotherDuck connection rejects `SET motherduck_token` after initialization and aliases in
    # workspace mode, so another MotherDuck database can only be attached into a local duckdb
    ATTACHABLE_TYPES: ClassVar[Tuple[TAttachType, ...]] = ("duckdb",)

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: MotherDuckCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(dataset_name, staging_dataset_name, credentials, capabilities)
        self.database_name = credentials.database

    def get_attach(self, *, alias: str) -> TAttachInfo:
        q_alias = self.escape_column_name(alias)
        attach = f"ATTACH IF NOT EXISTS 'md:{self.database_name}' AS {q_alias}"
        token = cast(MotherDuckCredentials, self.credentials).password
        if token:
            # attaching into a foreign connection needs the extension loaded and the token set
            # before ATTACH; only the token line is secret
            statements = [
                attach_statement("INSTALL motherduck"),
                attach_statement("LOAD motherduck"),
                attach_statement(f"SET motherduck_token='{token}'", secret=True),
                attach_statement(attach),
            ]
        else:
            statements = [attach_statement(attach)]
        return TAttachInfo(
            attach_type=self.attach_type,
            alias=alias,
            dataset_name=self.dataset_name,
            physical_location=f"md:{self.database_name}",
            statements=statements,
            detach_statements=[f"DETACH {q_alias}"],
        )

    def catalog_name(self, quote: bool = True, casefold: bool = True) -> Optional[str]:
        if casefold:
            database_name = self.capabilities.casefold_identifier(self.database_name)
        else:
            database_name = self.database_name
        if quote:
            database_name = self.capabilities.escape_identifier(database_name)
        return database_name
