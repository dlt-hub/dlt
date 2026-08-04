"""Query LanceDB over the Arrow Flight SQL endpoint of a managed cluster."""

from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AnyStr,
    ClassVar,
    Generator,
    Iterator,
    List,
    Optional,
    Sequence,
)

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.dataset import DBApiCursor
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.impl.lancedb import pyflightsql
from dlt.destinations.impl.lancedb.configuration import PUBLIC_SCHEMA_NAME, LanceDBCredentials
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    WithReadonlyClient,
    raise_database_error,
    raise_open_connection_error,
)
from dlt.destinations.typing import DBApi, DBTransaction

if TYPE_CHECKING:
    from pyarrow import Table as ArrowTable

UNDEFINED_RELATION_MESSAGES = (
    "not found",
    "no field named",
    "does not exist",
)
TRANSIENT_MESSAGES = ("unavailable", "deadline exceeded", "timed out", "connection reset")


class LanceDBCursorImpl(DBApiCursorImpl):
    """Reads Flight SQL results as arrow, so `df()` and `arrow()` skip the row tuple round trip."""

    native_cursor: pyflightsql.FlightSqlCursor  # type: ignore[assignment]

    def iter_arrow(self, chunk_size: Optional[int] = None) -> Generator["ArrowTable", None, None]:
        yield from self.native_cursor.iter_arrow_tables(chunk_size)


class LanceDBSqlClient(SqlClientBase[pyflightsql.FlightSqlConnection], WithReadonlyClient):
    dbapi: ClassVar[DBApi] = pyflightsql

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: LanceDBCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        # the dataset is the database, so it is also the catalog
        super().__init__(dataset_name, dataset_name, staging_dataset_name, capabilities)
        self.credentials = credentials
        self._conn: Optional[pyflightsql.FlightSqlConnection] = None

    @raise_open_connection_error
    def open_connection(self) -> pyflightsql.FlightSqlConnection:
        if not self.credentials.has_flightsql:
            raise DestinationTerminalException(
                "Reading from LanceDB requires the Arrow Flight SQL endpoint of your cluster. Set"
                " `destination.lancedb.credentials.flightsql_host` to enable SQL access."
            )
        self._conn = pyflightsql.connect(
            self.credentials.flightsql_uri(),
            headers=self.credentials.flightsql_headers(self.dataset_name),
        )
        return self._conn

    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def native_connection(self) -> pyflightsql.FlightSqlConnection:
        return self._conn

    @contextmanager
    def begin_transaction(self) -> Iterator[DBTransaction]:
        # the endpoint executes one statement per request and exposes no transactions
        yield self

    def commit_transaction(self) -> None:
        pass

    def rollback_transaction(self) -> None:
        pass

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            return None if curr.description is None else curr.fetchall()

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        curr = self._conn.cursor()
        try:
            curr.execute(query, args or None)  # type: ignore[arg-type]
            yield LanceDBCursorImpl(curr)  # type: ignore[arg-type]
        finally:
            curr.close()

    def catalog_name(self, quote: bool = True, casefold: bool = True) -> Optional[str]:
        if not self.database_name:
            return None
        database_name = (
            self.capabilities.casefold_identifier(self.database_name)
            if casefold
            else self.database_name
        )
        return self.capabilities.escape_identifier(database_name) if quote else database_name

    def make_qualified_table_name_path(
        self,
        table_name: Optional[str],
        quote: bool = True,
        casefold: bool = True,
        dataset_name: Optional[str] = None,
        catalog: Optional[str] = None,
    ) -> List[str]:
        """Returns `[database, "public", table]` for a table of `dataset_name` or of this dataset.

        Args:
            table_name: Table to address, optional.
            quote: Whether to escape each component.
            casefold: Whether to casefold each component.
            dataset_name: Dataset to address instead of this one, which is how references to another
                dataset are qualified.
            catalog: Catalog to address instead of the dataset. A dataset is a database here, so it
                already is the catalog and this replaces it.

        Returns:
            List[str]: Path components from catalog to table.
        """

        def render(identifier: str) -> str:
            if casefold:
                identifier = self.capabilities.casefold_identifier(identifier)
            return self.capabilities.escape_identifier(identifier) if quote else identifier

        database = catalog or dataset_name or self.dataset_name
        path = [render(database), render(PUBLIC_SCHEMA_NAME)]
        if table_name:
            path.append(render(table_name))
        return path

    def has_dataset(self) -> bool:
        # the sentinel namespace recording a dataset is only visible to the managed client
        return True

    def create_dataset(self) -> None:
        raise NotImplementedError(
            "LanceDB datasets are created through the managed client, not over SQL."
        )

    def drop_dataset(self) -> None:
        raise NotImplementedError(
            "LanceDB datasets are dropped through the managed client, not over SQL."
        )

    def drop_tables(self, *tables: str) -> None:
        raise NotImplementedError(
            "LanceDB tables are dropped through the managed client, not over SQL."
        )

    def truncate_tables(self, *tables: str) -> None:
        raise NotImplementedError(
            "LanceDB tables are truncated through the managed client, not over SQL."
        )

    @staticmethod
    def _make_database_exception(ex: Exception) -> Exception:
        message = str(ex).lower()
        if any(fragment in message for fragment in UNDEFINED_RELATION_MESSAGES):
            return DatabaseUndefinedRelation(ex)
        if any(fragment in message for fragment in TRANSIENT_MESSAGES):
            return DatabaseTransientException(ex)
        return DatabaseTerminalException(ex)
