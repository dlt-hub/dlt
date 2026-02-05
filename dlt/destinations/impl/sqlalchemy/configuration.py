import os
import threading
import warnings
from typing import TYPE_CHECKING, ClassVar, List, Optional, Any, Final, Type, Dict, Union
import dataclasses

from dlt.common import logger
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.configuration.specs.base_configuration import NotResolved
from dlt.common.destination.client import DestinationClientDwhConfiguration
from dlt.common.storages.configuration import WithLocalFiles
from dlt.common.typing import Annotated
from dlt.common.warnings import DltDeprecationWarning

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine, Dialect, Connection


class ManagedEngine:
    """Thread-safe engine lifecycle with ref-counted connection borrowing.

    Owned engines (created from URL) are disposed when the last borrowed
    connection is returned. External engines are never disposed.
    """

    def __init__(
        self,
        credentials: "SqlalchemyCredentials",
        engine: Optional["Engine"] = None,
    ) -> None:
        self._credentials = credentials
        self._conn_lock = threading.RLock()
        self._conn_borrows: int = 0
        if engine is not None:
            # external engine — not owned, not disposed by us
            self._engine: Optional["Engine"] = engine
            self._conn_owner: bool = False
        else:
            # owned engine — lazily created, disposed when refcount hits 0
            self._engine = None
            self._conn_owner = True

    @property
    def engine(self) -> "Engine":
        """Lazily creates the engine for owned instances, returns external engine otherwise."""
        import sqlalchemy as sa

        if self._engine is None:
            engine_kwargs = self._credentials._resolve_engine_kwargs()
            self._engine = sa.create_engine(
                self._credentials.to_url().render_as_string(hide_password=False),
                **engine_kwargs,
            )
        return self._engine

    def borrow_conn(self) -> "Connection":
        """Borrow a connection from the engine. Must be returned via return_conn."""
        with self._conn_lock:
            engine_ = self.engine
            self._conn_borrows += 1
        try:
            return engine_.connect()
        except Exception:
            with self._conn_lock:
                self._conn_borrows -= 1
                if self._conn_borrows == 0 and self._conn_owner:
                    self._dispose_engine()
            raise

    def return_conn(self, borrowed_conn: "Connection") -> None:
        """Return a borrowed connection. Disposes owned engine when refcount hits 0."""
        with self._conn_lock:
            borrowed_conn.close()
            assert self._conn_borrows > 0, "Returning connection when borrows is 0"
            self._conn_borrows -= 1
            if self._conn_borrows == 0 and self._conn_owner:
                self._dispose_engine()

    def _dispose_engine(self) -> None:
        if self._conn_borrows > 0:
            warnings.warn(
                f"Disposing engine {self._engine.url} with {self._conn_borrows} open conns."
            )
        if self._engine:
            self._engine.dispose()
            self._engine = None

    def __del__(self) -> None:
        if getattr(self, "_engine", None) and getattr(self, "_conn_owner", False):
            self._dispose_engine()


@configspec(init=False)
class SqlalchemyCredentials(ConnectionStringCredentials):
    engine_kwargs: Optional[Dict[str, Any]] = None
    """Additional keyword arguments passed to `sqlalchemy.create_engine`"""

    engine_args: Optional[Dict[str, Any]] = None
    """DEPRECATED: use engine_kwargs instead"""

    managed_engine: Annotated[Optional[ManagedEngine], NotResolved()] = None

    def __init__(
        self, connection_string: Optional[Union[str, Dict[str, Any], "Engine"]] = None
    ) -> None:
        super().__init__(connection_string)  # type: ignore[arg-type]

    def copy(self: "SqlalchemyCredentials") -> "SqlalchemyCredentials":
        new_obj = super().copy()
        # copy always holds the engine as unmanaged (never disposes)
        if self.managed_engine is not None and self.managed_engine._engine is not None:
            new_obj.managed_engine = ManagedEngine(new_obj, engine=self.managed_engine._engine)
        else:
            new_obj.managed_engine = None
        return new_obj

    def _resolve_engine_kwargs(self) -> Dict[str, Any]:
        if self.engine_kwargs and self.engine_args:
            raise ValueError(
                "Both engine_kwargs and engine_args were provided. Use engine_kwargs only."
            )

        if self.engine_args and not self.engine_kwargs:
            warnings.warn(
                DltDeprecationWarning(
                    "`engine_args` is deprecated; use `engine_kwargs` instead",
                    since="1.21.0",
                ),
                stacklevel=2,
            )
            return self.engine_args

        return self.engine_kwargs or {}

    def parse_native_representation(self, native_value: Any) -> None:
        from sqlalchemy.engine import Engine

        if isinstance(native_value, Engine):
            # triggers setter which creates ManagedEngine wrapping external engine
            self.engine = native_value
            super().parse_native_representation(
                native_value.url.render_as_string(hide_password=False)
            )
        else:
            super().parse_native_representation(native_value)

    def _ensure_managed_engine(self) -> ManagedEngine:
        """Lazily create ManagedEngine on first access."""
        if self.managed_engine is None:
            external = getattr(self, "_external_engine", None)
            self.managed_engine = ManagedEngine(self, engine=external)
        return self.managed_engine

    @property
    def engine(self) -> Optional["Engine"]:
        return self._ensure_managed_engine().engine

    @engine.setter
    def engine(self, value: "Engine") -> None:
        # backward compat: create ManagedEngine wrapping external engine
        self._external_engine = value
        self.managed_engine = ManagedEngine(self, engine=value)

    def get_dialect(self) -> Optional[Type["Dialect"]]:
        if not self.drivername:
            return None
        # Type-ignore because of ported URL class has no get_dialect method,
        # but here sqlalchemy should be available
        if engine := self.engine:
            return type(engine.dialect)
        return self.to_url().get_dialect()  # type: ignore[attr-defined,no-any-return]

    def get_backend_name(self) -> str:
        if not self.drivername:
            return None
        return self.to_url().get_backend_name()

    @staticmethod
    def is_memory_database(database: Optional[str], query: Optional[Dict[str, Any]] = None) -> bool:
        """Detect if the given database/query represent an in-memory SQLite database.

        Handles the following forms:
        - Classic: ``:memory:``
        - URI: ``file:<name>?mode=memory&cache=shared&uri=true``

        NOTE: empty/None database is NOT considered in-memory here because dlt's
        configuration system resolves it to a default file path.
        """
        if database == ":memory:":
            return True
        # URI format: file:<name> with mode=memory and uri=true in query params
        # uri=true is required for pysqlite to interpret the database as a URI
        if (
            database
            and database.startswith("file:")
            and query
            and query.get("mode") == "memory"
            and str(query.get("uri", "")).lower() == "true"
        ):
            return True
        return False

    __config_gen_annotations__: ClassVar[List[str]] = [
        "database",
        "port",
        "username",
        "password",
        "host",
    ]


SQLITE_DB_NAME_PAT = "%s.db"


@configspec
class SqlalchemyClientConfiguration(WithLocalFiles, DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(default="sqlalchemy", init=False, repr=False, compare=False)  # type: ignore
    credentials: SqlalchemyCredentials = None
    """SQLAlchemy connection string"""
    create_unique_indexes: bool = False
    """Whether UNIQUE constrains should be created"""
    create_primary_keys: bool = False
    """Whether PRIMARY KEY constrains should be created"""

    engine_kwargs: Dict[str, Any] = dataclasses.field(default_factory=dict)
    """Additional keyword arguments passed to `sqlalchemy.create_engine`"""

    engine_args: Dict[str, Any] = dataclasses.field(default_factory=dict)
    """DEPRECATED: use engine_kwargs instead"""

    def get_dialect(self) -> Type["Dialect"]:
        return self.credentials.get_dialect()

    def get_backend_name(self) -> str:
        return self.credentials.get_backend_name()

    def on_resolved(self) -> None:
        if self.engine_kwargs and self.engine_args:
            raise ValueError(
                "Both engine_kwargs and engine_args were provided. Use engine_kwargs only."
            )

        if self.engine_args and not self.engine_kwargs:
            warnings.warn(
                DltDeprecationWarning(
                    "`engine_args` is deprecated; use `engine_kwargs` instead",
                    since="1.21.0",
                ),
                stacklevel=2,
            )
            self.engine_kwargs = self.engine_args

        if self.engine_kwargs and not self.credentials.engine_kwargs:
            self.credentials.engine_kwargs = self.engine_kwargs

        # resolve local file path for sqlite backends
        if self.credentials.drivername == "sqlite":
            if not SqlalchemyCredentials.is_memory_database(
                self.credentials.database, self.credentials.query
            ):
                db = self.credentials.database
                if not db or not os.path.isabs(db):
                    self.credentials.database = os.path.normpath(
                        self.make_location(db or None, SQLITE_DB_NAME_PAT)
                    )
