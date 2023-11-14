import os
import threading
from pathvalidate import is_valid_filepath
from typing import Any, ClassVar, Final, List, Optional, Tuple, TYPE_CHECKING, Union

from dlt.common import logger
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.configuration.specs.exceptions import InvalidConnectionString
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration, DestinationClientStagingConfiguration
from dlt.common.typing import TSecretValue

DUCK_DB_NAME = "%s.duckdb"
DEFAULT_DUCK_DB_NAME = DUCK_DB_NAME % "quack"
LOCAL_STATE_KEY = "duckdb_database"


@configspec
class DuckDbBaseCredentials(ConnectionStringCredentials):
    password: Optional[TSecretValue] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None

    read_only: bool = False  # open database read/write

    def borrow_conn(self, read_only: bool) -> Any:
        import duckdb

        if not hasattr(self, "_conn_lock"):
            self._conn_lock = threading.Lock()

        # obtain a lock because duck releases the GIL and we have refcount concurrency
        with self._conn_lock:
            if not hasattr(self, "_conn"):
                self._conn = duckdb.connect(database=self._conn_str(), read_only=read_only)
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

    def _conn_str(self) -> str:
        return self.database

    def _delete_conn(self) -> None:
        # print("Closing conn because is owner")
        self._conn.close()
        delattr(self, "_conn")

    def __del__(self) -> None:
        if hasattr(self, "_conn") and self._conn_owner:
            self._delete_conn()


@configspec
class DuckDbCredentials(DuckDbBaseCredentials):
    drivername: Final[str] = "duckdb"  # type: ignore
    username: Optional[str] = None

    __config_gen_annotations__: ClassVar[List[str]] = []

    def on_resolved(self) -> None:
        # do not set any paths for external database
        if self.database == ":external:":
            return
        # try the pipeline context
        is_default_path = False
        if self.database == ":pipeline:":
            self.database = self._path_in_pipeline(DEFAULT_DUCK_DB_NAME)
        else:
            # maybe get database
            maybe_database, maybe_is_default_path = self._path_from_pipeline(DEFAULT_DUCK_DB_NAME)
            # if pipeline context was not present or database was not set
            if not self.database or not maybe_is_default_path:
                # create database locally
                is_default_path = maybe_is_default_path
                self.database = maybe_database

        # always make database an abs path
        self.database = os.path.abspath(self.database)
        # do not save the default path into pipeline's local state
        if not is_default_path:
            self._path_to_pipeline(self.database)

    def _path_in_pipeline(self, rel_path: str) -> str:
        from dlt.common.configuration.container import Container
        from dlt.common.pipeline import PipelineContext

        context = Container()[PipelineContext]
        if context.is_active():
            # pipeline is active, get the working directory
            return os.path.join(context.pipeline().working_dir, rel_path)
        return None


    def _path_to_pipeline(self, abspath: str) -> None:
        from dlt.common.configuration.container import Container
        from dlt.common.pipeline import PipelineContext

        context = Container()[PipelineContext]
        if context.is_active():
            context.pipeline().set_local_state_val(LOCAL_STATE_KEY, abspath)

    def _path_from_pipeline(self, default_path: str) -> Tuple[str, bool]:
        """
        Returns path to DuckDB as stored in the active pipeline's local state and a boolean flag.

        If the pipeline state is not available, returns the default DuckDB path that includes the pipeline name and sets the flag to True.
        If the pipeline context is not available, returns the provided default_path and sets the flag to True.

        Args:
            default_path (str): The default DuckDB path to return if the pipeline context or state is not available.

        Returns:
            Tuple[str, bool]: The path to the DuckDB as stored in the active pipeline's local state or the default path if not available,
            and a boolean flag set to True when the default path is returned.
        """
        from dlt.common.configuration.container import Container
        from dlt.common.pipeline import PipelineContext

        context = Container()[PipelineContext]
        if context.is_active():
            try:
                # use pipeline name as default
                pipeline = context.pipeline()
                default_path = DUCK_DB_NAME % pipeline.pipeline_name
                # get pipeline path from local state
                pipeline_path = pipeline.get_local_state_val(LOCAL_STATE_KEY)
                # make sure that path exists
                if not os.path.exists(pipeline_path):
                    logger.warning(f"Duckdb attached to pipeline {pipeline.pipeline_name} in path {os.path.relpath(pipeline_path)} was deleted. Attaching to duckdb database '{default_path}' in current folder.")
                else:
                    return pipeline_path, False
            except KeyError:
                # no local state: default_path will be used
                pass

        return default_path, True


@configspec
class DuckDbClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_name: Final[str] = "duckdb"  # type: ignore
    credentials: DuckDbCredentials

    create_indexes: bool = False  # should unique indexes be created, this slows loading down massively

    if TYPE_CHECKING:
        try:
            from duckdb import DuckDBPyConnection
        except ModuleNotFoundError:
            DuckDBPyConnection = Any  # type: ignore[assignment,misc]

        def __init__(
            self,
            destination_name: str = None,
            credentials: Union[DuckDbCredentials, str, DuckDBPyConnection] = None,
            dataset_name: str = None,
            default_schema_name: Optional[str] = None,
            create_indexes: bool = False,
            staging_config: Optional[DestinationClientStagingConfiguration] = None
        ) -> None:
            ...
