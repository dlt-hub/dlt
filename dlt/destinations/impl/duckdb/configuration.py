import dataclasses
import threading
from typing import (
    Any,
    ClassVar,
    Dict,
    Final,
    List,
    Literal,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Union,
    TYPE_CHECKING,
)
from pathvalidate import is_valid_filepath

from dlt.common.configuration import configspec
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration, NotResolved
from dlt.common.configuration.specs.exceptions import InvalidConnectionString
from dlt.common.destination.attach import TAttachType
from dlt.common.destination.client import (
    DestinationClientConfiguration,
    DestinationClientDwhWithStagingConfiguration,
    WithAttachableEngine,
)
from dlt.common.storages import WithLocalFiles
from dlt.common.typing import Annotated, SecretSentinel
from dlt.common.utils import merge_keyed_groups

from dlt.destinations.impl.duckdb.exceptions import InvalidInMemoryDuckdbCredentials

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
else:
    DuckDBPyConnection = Any  # type: ignore[assignment,misc]

DUCK_DB_NAME_PAT = "%s.duckdb"
NON_ATTACHABLE_LOCATIONS = (":memory:", ":external:")
"""Databases that live inside a single connection. No other connection can attach them."""


class ConnStatement(NamedTuple):
    """A database-scoped statement a pool runs on each connection it opens"""

    # runs on the connection the pool keeps, so `SET SESSION` and `USE` do not apply to the
    # sessions cloned from it. `pragmas` and `local_config` hold those
    sql: str
    key: Optional[str] = None
    """What the statement configures. The default is the SQL itself. The pool replaces all
    statements with the same key as one group"""


@configspec(init=False)
class DuckDbBaseCredentials(CredentialsConfiguration):
    read_only: bool = False
    """Open database r or rw"""
    extensions: Optional[List[str]] = None
    """Extensions loaded on each newly opened connection"""
    global_config: Optional[Dict[str, Any]] = None
    """Global config applied once on each newly opened connection"""
    pragmas: Optional[List[str]] = None
    """Pragmas set applied to each borrowed connection"""
    statements: Annotated[Optional[List[str]], SecretSentinel] = None
    """Database-scoped SQL run on each newly opened connection, after `extensions` and
    `global_config`. This field holds the statements those two fields cannot express, such as
    `INSTALL`, `ATTACH` and `CREATE SECRET`"""
    local_config: Optional[Dict[str, Any]] = None
    """Local config applied to each borrowed connection"""
    session_timezone: Optional[str] = "UTC"
    """`TimeZone` set on each newly opened connection, which its sessions inherit. `None` keeps
    the duckdb default"""
    conn_pool: Annotated[Optional["DuckDbConnectionPool"], NotResolved()] = None

    def external_conn(self) -> Optional[DuckDBPyConnection]:
        """Returns the connection that the caller passed, `None` when dlt opens its own."""
        if conn := getattr(self, "_external_conn", None):
            return conn  # type: ignore[no-any-return]
        if self.conn_pool is not None and not self.conn_pool._conn_owner:
            return self.conn_pool._conn
        return None

    def copy(self: "DuckDbBaseCredentials") -> "DuckDbBaseCredentials":
        new_obj = super().copy()
        # conn_pool holds threading state that must not be shared across copies
        if conn := self.external_conn():
            # external connection: set _external_conn so pool constructor picks it up
            new_obj._external_conn = conn
            new_obj.conn_pool = DuckDbConnectionPool(new_obj)
        else:
            # owned connection: let on_resolved() create a fresh pool
            new_obj.conn_pool = None
        return new_obj

    def parse_native_representation(self, native_value: Any) -> None:
        if isinstance(native_value, DuckDbBaseCredentials):
            # the resolver copies only the fields of a credentials instance passed to a factory,
            # so the caller's connection would be lost
            if conn := native_value.external_conn():
                self._external_conn = conn
                self.database = self._external_conn_database(conn)
            return
        try:
            # check if database was passed as explicit connection
            import duckdb

            if isinstance(native_value, duckdb.DuckDBPyConnection):
                self._external_conn = native_value
                self.database = self._external_conn_database(native_value)
                return
        except ImportError:
            pass
        try:
            super().parse_native_representation(native_value)
        except InvalidConnectionString:
            if native_value == ":pipeline:" or is_valid_filepath(native_value, platform="auto"):
                self.database = native_value
            else:
                raise

    @staticmethod
    def _external_conn_database(conn: DuckDBPyConnection) -> str:
        """Returns the file path of the connection's current database so data_location()
        identifies it, ':external:' for in-memory databases or when the path cannot be read.
        """
        try:
            cur = conn.cursor()
            try:
                row = cur.execute(
                    "select path from duckdb_databases() where database_name = current_database()"
                ).fetchone()
            finally:
                cur.close()
            if row and row[0]:
                return str(row[0])
        except Exception:
            pass
        return ":external:"

    def _conn_str(self) -> str:
        raise NotImplementedError()


class DuckDbConnectionPool:
    always_open_connection: bool
    """Always opens a new connection without cloning with cursor"""

    def __init__(self, credentials: DuckDbBaseCredentials, always_open_connection: bool = False):
        """Initializes a connection pool that dispenses duckdb connection to be used in multiple threads.

        Default mode of the operation is to create a single duckdb connection and then use `duplicate`
        method to pass a connection clone to a thread.

        With `always_open_connection`, thread receives a new duckdb connection every time primarily
        to support attached databases like ducklake. Current implementation does not pool connections
        in this mode, it creates a fresh copy on each request.

        This mechanism is piggybacking on destination Configuration/Credentials which are a singleton
        in pipeline in load step. This allows to dispense connections in to workers in multiple
        threads.
        """
        self.credentials = credentials
        self.always_open_connection = always_open_connection
        self._conn_lock = threading.RLock()
        self._conn_borrows = 0
        self._conn: DuckDBPyConnection = None
        self._statements: List[ConnStatement] = []
        self.attached_aliases: Set[str] = set()
        """Attach aliases already registered on this pool. All its sql clients share them"""
        if external_conn := getattr(credentials, "_external_conn", None):
            if self.always_open_connection:
                raise ConfigurationValueError("External connections not supported")
            self._conn = external_conn
            self._conn_owner = False
        else:
            # connections are externally owned when always_open_connection
            self._conn_owner = True
            self._conn = None
        if credentials.statements:
            self._statements = [ConnStatement(sql) for sql in credentials.statements]

    def borrow_conn(
        self,
        global_config: Dict[str, Any] = None,
        local_config: Dict[str, Any] = None,
        pragmas: List[str] = None,
    ) -> DuckDBPyConnection:
        """Opens new or clones existing duckdb connection to support multi-thread access and then
        borrows it to the caller. Caller is supposed to return the connection when it is no longer
        needed. If connection is not returned the underlying duckdb conn will never be closed due to
        internal ref counting.
        """
        import duckdb

        # obtain a lock because duck releases the GIL and we have refcount concurrency
        with self._conn_lock:
            # calculate global config
            global_config = {**(self.credentials.global_config or {}), **(global_config or {})}
            # extract configs that must be passed to connect
            connect_config = {}
            for key in list(global_config.keys()):
                if key in ("custom_user_agent",):
                    connect_config[key] = global_config.pop(key)

            if self._conn is None:
                new_conn = duckdb.connect(
                    database=self.credentials._conn_str(),
                    read_only=self.credentials.read_only,
                    config=connect_config,
                )
            else:
                new_conn = self._conn

            # if connection is borrowed for the first time, load extensions and set global settings
            if self._conn_borrows == 0 or new_conn != self._conn:
                try:
                    # load extensions in config
                    if self.credentials.extensions:
                        for extension in self.credentials.extensions:
                            new_conn.sql(f"LOAD {extension}")

                    self._apply_config(new_conn, "GLOBAL", global_config)
                    # before local config: a statement can create the schema that `search_path` names
                    self._execute_statements(new_conn)
                    self._apply_local_config(new_conn, local_config, pragmas)
                except Exception:
                    if self._conn_owner:
                        new_conn.close()
                    raise

            # remember duckdb connection, except if you open new one on each borrow
            if not self.always_open_connection:
                self._conn = new_conn
                # do not return original connection but a clone
                new_conn = new_conn.duplicate()

            # print(f"getting conn refcnt {self._conn_borrows} at {id(self)}")
            # track open connections to properly close it
            self._conn_borrows += 1

            try:
                self._apply_local_config(new_conn, local_config, pragmas)
            except Exception:
                # will refcount down and close cursor or conn
                self.return_conn(new_conn)
                raise
            return new_conn

    def add_statements(
        self,
        statements: Sequence[ConnStatement],
        alias: str = None,
        conn: DuckDBPyConnection = None,
    ) -> List[ConnStatement]:
        """Registers `statements` to run on each connection this pool opens. Returns the
        statements that are new.

        Args:
            statements: Database-scoped statements, see `ConnStatement`.
            alias: The attach catalog that the statements add. The pool keeps it in
                `attached_aliases`.
            conn: The connection that gets the statements at once. This argument is necessary
                only with `always_open_connection`, where the pool keeps no connection of its own.
        """
        with self._conn_lock:
            # an unkeyed statement takes its own SQL as the key, so two identical statements
            # merge into one
            merged, added = merge_keyed_groups(
                self._statements, statements, lambda s: s.key or s.sql
            )
            if added:
                if conn := conn or self._conn:
                    for statement in added:
                        conn.execute(statement.sql)
                # recorded last: the pool must not replay a statement which failed to run
                self._statements = merged
            if alias:
                self.attached_aliases.add(alias)
            return added

    def _execute_statements(self, conn: DuckDBPyConnection) -> None:
        for statement in self._statements:
            conn.execute(statement.sql)

    def return_conn(self, borrowed_conn: DuckDBPyConnection) -> int:
        """Closed the borrowed conn, if refcount goes to 0, duckdb connection is deleted"""
        borrowed_conn.close()

        with self._conn_lock:
            # close the main conn if the last borrowed conn was closed
            assert self._conn_borrows > 0, "Returning connection when borrows is 0"
            self._conn_borrows -= 1
            if self._conn_borrows == 0 and self._conn_owner:
                self._close_conn()
        return self._conn_borrows

    def move_conn(self) -> DuckDBPyConnection:
        """Takes ownership of the connection so it won't be closed on refcount 0 and in destructor"""
        if self.always_open_connection:
            raise NotImplementedError(
                "Moving ownership not implemented for always_open_connection=True"
            )
        assert self._conn is not None, "Connection is not opened"
        self._conn_owner = False
        return self._conn

    def _apply_local_config(
        self,
        conn: DuckDBPyConnection,
        local_config: Dict[str, Any] = None,
        pragmas: List[str] = None,
    ) -> None:
        # set pragmas
        pragmas = [*(self.credentials.pragmas or {}), *(pragmas or {})]
        for pragma in pragmas:
            conn.sql(f"PRAGMA {pragma}")
        # calculate local config
        local_config = {**(self.credentials.local_config or {}), **(local_config or {})}
        self._apply_config(conn, "SESSION", local_config)

    @staticmethod
    def _apply_config(
        conn: DuckDBPyConnection, scope: Literal["GLOBAL", "SESSION"], config: Dict[str, Any]
    ) -> None:
        import duckdb

        for k, v in config.items():
            try:
                try:
                    conn.execute(f"SET {scope} {k} = ?", (v,))
                except (
                    duckdb.BinderException,
                    duckdb.ParserException,
                    duckdb.InvalidInputException,
                ):
                    # binders do not work on motherduck and old versions of duckdb
                    if isinstance(v, str):
                        v = f"'{v}'"
                    conn.execute(f"SET {scope} {k} = {v}")

            except duckdb.CatalogException:
                # allow search_path to fail if path does not exist
                if k == "search_path":
                    pass
                else:
                    raise

    def _close_conn(self) -> None:
        if self._conn:
            # duckdb allow to call close on closed connection without error
            self._conn.close()
            self._conn = None

    def __del__(self) -> None:
        if self._conn and self._conn_owner:
            self._close_conn()


@configspec
class DuckDbCredentials(DuckDbBaseCredentials, ConnectionStringCredentials):
    drivername: Final[str] = dataclasses.field(default="duckdb", init=False, repr=False, compare=False)  # type: ignore
    username: Optional[str] = None

    __config_gen_annotations__: ClassVar[List[str]] = []

    def on_resolved(self) -> None:
        if isinstance(self.database, str) and self.database == ":memory:":
            raise InvalidInMemoryDuckdbCredentials()
        self.conn_pool = DuckDbConnectionPool(self)

    def _conn_str(self) -> str:
        # if not self.database or not os.path.abspath(self.database):
        #     self.setup_database()
        return self.database

    def __init__(
        self,
        conn_or_path: Union[str, DuckDBPyConnection] = None,
        *,
        read_only: bool = False,
        extensions: Optional[List[str]] = None,
        global_config: Optional[Dict[str, Any]] = None,
        pragmas: Optional[List[str]] = None,
        statements: Optional[List[str]] = None,
        local_config: Optional[Dict[str, Any]] = None,
        session_timezone: Optional[str] = "UTC",
    ) -> None:
        """Initialize DuckDB credentials with a connection or file path and connection settings.

        Args:
            conn_or_path: Either a DuckDB connection object or a path to a DuckDB database file.
                          Can also be special values like ':pipeline:' or ':memory:'.
            read_only: Open database in read-only mode if True, read-write mode if False
            extensions: List of DuckDB extensions to load on each newly opened connection
            global_config: Dictionary of global configuration settings applied once on each newly opened connection
            pragmas: List of PRAGMA statements to be applied to each cursor connection
            statements: Database-scoped SQL run on each newly opened connection, for example
                `INSTALL`, `ATTACH` and `CREATE SECRET`. Session settings belong in `pragmas`
                or `local_config`
            local_config: Dictionary of local configuration settings applied to each cursor connection
            session_timezone: `TimeZone` set on each newly opened connection, which its cursor
                connections inherit. `None` keeps the duckdb default
        """
        self._apply_init_value(conn_or_path)
        self.read_only = read_only
        self.extensions = extensions
        self.global_config = global_config
        self.pragmas = pragmas
        self.statements = statements
        self.local_config = local_config
        self.session_timezone = session_timezone


@configspec
class DuckDbClientConfiguration(
    WithAttachableEngine, WithLocalFiles, DestinationClientDwhWithStagingConfiguration
):
    destination_type: Final[str] = dataclasses.field(default="duckdb", init=False, repr=False, compare=False)  # type: ignore
    credentials: DuckDbCredentials = None
    create_indexes: bool = (
        False  # should unique indexes be created, this slows loading down massively
    )

    def __init__(
        self,
        *,
        credentials: Union[DuckDbCredentials, str, DuckDBPyConnection] = None,
        create_indexes: bool = False,
        destination_name: str = None,
        environment: str = None,
    ) -> None:
        super(DestinationClientDwhWithStagingConfiguration, self).__init__(
            credentials=credentials,  # type: ignore[arg-type]
            destination_name=destination_name,
            environment=environment,
        )
        self.create_indexes = create_indexes

    def data_location(self) -> str:
        """Returns the database file path. For a database that lives inside a query engine,
        returns the marker of that database and the identity of the engine."""
        if not self.credentials or not self.credentials.database:
            self._no_data_location("the configuration has no database")
        database = self.credentials.database
        if database not in NON_ATTACHABLE_LOCATIONS:
            return database
        # the marker identifies no database, so the query engine that holds it is the only
        # identity. markers alone make any two in-memory databases look like one
        conn = getattr(self.credentials, "_external_conn", None)
        if conn is None and self.credentials.conn_pool:
            conn = self.credentials.conn_pool._conn
        if conn is None:
            self._no_data_location(f"`{database}` has no open connection that identifies it")
        return f"{database}{hex(id(conn))}"

    def needs_attach(self, other: DestinationClientConfiguration) -> bool:
        """Returns False for a database that this query engine already opened. The engine
        accesses every schema of that database."""
        return not self.is_same_location(other)

    def attach_type(self) -> Optional[TAttachType]:
        """Returns None for a database that lives inside a query engine. Such a database has no
        path to attach."""
        if self.credentials and self.credentials.database in NON_ATTACHABLE_LOCATIONS:
            return None
        return super().attach_type()

    def on_resolved(self) -> None:
        self.credentials.database = self.make_location(self.credentials.database, DUCK_DB_NAME_PAT)
