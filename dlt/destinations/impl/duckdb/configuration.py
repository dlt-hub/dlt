import dataclasses
import threading
from typing import Any, ClassVar, Dict, Final, List, Optional, Type, Union
from pathvalidate import is_valid_filepath

import dlt.common
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.configuration.specs.exceptions import InvalidConnectionString
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration

from dlt.destinations.configuration import WithLocalFiles
from dlt.destinations.impl.duckdb.exceptions import InvalidInMemoryDuckdbCredentials

try:
    from duckdb import DuckDBPyConnection
except ModuleNotFoundError:
    DuckDBPyConnection = Type[Any]  # type: ignore[assignment,misc]

DUCK_DB_NAME_PAT = "%s.duckdb"


@configspec(init=False)
class DuckDbBaseCredentials(ConnectionStringCredentials):
    read_only: bool = False  # open database read/write

    def borrow_conn(self, read_only: bool) -> Any:
        import duckdb

        if not hasattr(self, "_conn_lock"):
            self._conn_lock = threading.Lock()

        config = self._get_conn_config()
        # obtain a lock because duck releases the GIL and we have refcount concurrency
        with self._conn_lock:
            if not hasattr(self, "_conn"):
                self._conn = duckdb.connect(
                    database=self._conn_str(), read_only=read_only, config=config
                )
                self._conn_owner = True
                self._conn_borrows = 0

            # track open connections to properly close it
            self._conn_borrows += 1
            # print(f"getting conn refcnt {self._conn_borrows} at {id(self)}")
            return self._conn.cursor()

    def return_conn(self, borrowed_conn: Any) -> None:
        # print(f"returning conn refcnt {self._conn_borrows} at {id(self)}")
        # close the borrowed conn
        borrowed_conn.close()

        with self._conn_lock:
            # close the main conn if the last borrowed conn was closed
            assert self._conn_borrows > 0, "Returning connection when borrows is 0"
            self._conn_borrows -= 1
            if self._conn_borrows == 0 and self._conn_owner:
                self._delete_conn()

    def parse_native_representation(self, native_value: Any) -> None:
        try:
            # check if database was passed as explicit connection
            import duckdb

            if isinstance(native_value, duckdb.DuckDBPyConnection):
                self._conn = native_value
                self._conn_owner = False
                self._conn_borrows = 0
                self.database = ":external:"
                self.__is_resolved__ = True
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

    def _get_conn_config(self) -> Dict[str, Any]:
        return {}

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

    def __init__(self, conn_or_path: Union[str, DuckDBPyConnection] = None) -> None:
        """Access to duckdb database at a given path or from duckdb connection"""
        self._apply_init_value(conn_or_path)


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
        super().__init__(
            credentials=credentials,  # type: ignore[arg-type]
            destination_name=destination_name,
            environment=environment,
        )
        self.create_indexes = create_indexes

    def on_resolved(self) -> None:
        self.credentials.database = self.make_location(self.credentials.database, DUCK_DB_NAME_PAT)
