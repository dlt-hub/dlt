import dataclasses
import threading
from typing import Any, ClassVar, Dict, Final, List, Literal, Optional, Union, TYPE_CHECKING
from pathvalidate import is_valid_filepath

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.configuration.specs.exceptions import InvalidConnectionString
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.storages import WithLocalFiles

from dlt.destinations.impl.duckdb.exceptions import InvalidInMemoryDuckdbCredentials

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
else:
    DuckDBPyConnection = Any  # type: ignore[assignment,misc]

DUCK_DB_NAME_PAT = "%s.duckdb"


@configspec(init=False)
class DuckDbBaseCredentials(ConnectionStringCredentials):
    _LOCK: ClassVar[threading.Lock] = threading.Lock()

    read_only: bool = False
    """Open database r or rw"""
    extensions: Optional[List[str]] = None
    """Extensions loaded on each newly opened connection"""
    global_config: Optional[Dict[str, Any]] = None
    """Global config applied once on each newly opened connection"""
    pragmas: Optional[List[str]] = None
    """Pragmas set applied to each borrowed connection"""
    local_config: Optional[Dict[str, Any]] = None
    """Local config applied to each borrowed connection"""

    def borrow_conn(
        self,
        global_config: Dict[str, Any] = None,
        local_config: Dict[str, Any] = None,
        pragmas: List[str] = None,
    ) -> DuckDBPyConnection:
        import duckdb

        if not hasattr(self, "_conn_lock"):
            with DuckDbBaseCredentials._LOCK:
                if not hasattr(self, "_conn_lock"):
                    self._conn_lock = threading.Lock()

        # obtain a lock because duck releases the GIL and we have refcount concurrency
        with self._conn_lock:
            # calculate global config
            global_config = {**(self.global_config or {}), **(global_config or {})}
            # extract configs that must be passed to connect
            connect_config = {}
            for key in list(global_config.keys()):
                if key in ("custom_user_agent",):
                    connect_config[key] = global_config.pop(key)

            if not getattr(self, "_conn", None):
                self._conn = duckdb.connect(
                    database=self._conn_str(), read_only=self.read_only, config=connect_config
                )
                self._conn_owner = True
                self._conn_borrows = 0

            if self._conn_borrows == 0:
                try:
                    # load extensions in config
                    if self.extensions:
                        for extension in self.extensions:
                            self._conn.sql(f"LOAD {extension}")

                    self._apply_config(self._conn, "GLOBAL", global_config)
                    # apply local config to original connection
                    self._apply_local_config(self._conn, local_config, pragmas)
                except Exception:
                    if self._conn_owner:
                        self._delete_conn()
                    raise

            # print(f"getting conn refcnt {self._conn_borrows} at {id(self)}")
            cur = self._conn.cursor()
            try:
                self._apply_local_config(cur, local_config, pragmas)
                # track open connections to properly close it
                self._conn_borrows += 1
                return cur
            except Exception:
                cur.close()
                raise

    def return_conn(self, borrowed_conn: DuckDBPyConnection) -> int:
        """Closed the borrowed conn, if refcount goes to 0, duckdb connection is deleted"""
        borrowed_conn.close()

        with self._conn_lock:
            # close the main conn if the last borrowed conn was closed
            assert self._conn_borrows > 0, "Returning connection when borrows is 0"
            self._conn_borrows -= 1
            if self._conn_borrows == 0 and self._conn_owner:
                self._delete_conn()
        return self._conn_borrows

    def move_conn(self) -> DuckDBPyConnection:
        """Takes ownership of the connection so it won't be closed on refcount 0 and in destructor"""
        assert hasattr(self, "_conn"), "Connection is not opened"
        self._conn_owner = False
        return self._conn

    def parse_native_representation(self, native_value: Any) -> None:
        try:
            # check if database was passed as explicit connection
            import duckdb

            if isinstance(native_value, duckdb.DuckDBPyConnection):
                self._conn = native_value
                self._conn_owner = False
                self._conn_borrows = 0
                self.database = ":external:"
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

    @property
    def never_borrowed(self) -> bool:
        """Returns true if connection was not yet created or no connections were borrowed in case of external connection"""
        return not hasattr(self, "_conn") or self._conn_borrows == 0

    def _apply_local_config(
        self,
        conn: DuckDBPyConnection,
        local_config: Dict[str, Any] = None,
        pragmas: List[str] = None,
    ) -> None:
        # set pragmas
        pragmas = [*(self.pragmas or {}), *(pragmas or {})]
        for pragma in pragmas:
            conn.sql(f"PRAGMA {pragma}")
        # calculate local config
        local_config = {**(self.local_config or {}), **(local_config or {})}
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

    def _conn_str(self) -> str:
        raise NotImplementedError()

    def _delete_conn(self) -> None:
        self._conn.close()
        delattr(self, "_conn")

    def __del__(self) -> None:
        if hasattr(self, "_conn") and self._conn_owner:
            self._delete_conn()


@configspec
class DuckDbCredentials(DuckDbBaseCredentials):
    drivername: Final[str] = dataclasses.field(default="duckdb", init=False, repr=False, compare=False)  # type: ignore
    username: Optional[str] = None

    __config_gen_annotations__: ClassVar[List[str]] = []

    def is_partial(self) -> bool:
        partial = super().is_partial()
        if partial:
            return True
        # Wait until pipeline context is set up before resolving
        return self.database == ":pipeline:"

    def on_resolved(self) -> None:
        if isinstance(self.database, str) and self.database == ":memory:":
            raise InvalidInMemoryDuckdbCredentials()

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
        local_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize DuckDB credentials with a connection or file path and connection settings.

        Args:
            conn_or_path: Either a DuckDB connection object or a path to a DuckDB database file.
                          Can also be special values like ':pipeline:' or ':memory:'.
            read_only: Open database in read-only mode if True, read-write mode if False
            extensions: List of DuckDB extensions to load on each newly opened connection
            global_config: Dictionary of global configuration settings applied once on each newly opened connection
            pragmas: List of PRAGMA statements to be applied to each cursor connection
            local_config: Dictionary of local configuration settings applied to each cursor connection
        """
        self._apply_init_value(conn_or_path)
        self.read_only = read_only
        self.extensions = extensions
        self.global_config = global_config
        self.pragmas = pragmas
        self.local_config = local_config


@configspec
class DuckDbClientConfiguration(WithLocalFiles, DestinationClientDwhWithStagingConfiguration):
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

    def on_resolved(self) -> None:
        self.credentials.database = self.make_location(self.credentials.database, DUCK_DB_NAME_PAT)
