from contextlib import contextmanager
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence

import duckdb

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.duckdb.sql_client import DuckDBDBApiCursorImpl, DuckDbSqlClient
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.motherduck import capabilities
from dlt.destinations.motherduck.configuration import MotherDuckCredentials
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    raise_database_error,
    raise_open_connection_error,
)
from dlt.destinations.typing import DataFrame, DBApi, DBApiCursor, DBTransaction


class MotherDuckSqlClient(DuckDbSqlClient):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, dataset_name: str, credentials: MotherDuckCredentials) -> None:
        super().__init__(dataset_name, credentials)
        self.database_name = credentials.database

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        database_name = (
            self.capabilities.escape_identifier(self.database_name)
            if escape
            else self.database_name
        )
        dataset_name = (
            self.capabilities.escape_identifier(self.dataset_name) if escape else self.dataset_name
        )
        return f"{database_name}.{dataset_name}"
