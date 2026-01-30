import os
import threading
import warnings
from typing import TYPE_CHECKING, ClassVar, List, Optional, Any, Final, Type, Dict, Union
import dataclasses

from dlt.common import logger
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.destination.client import DestinationClientDwhConfiguration
from dlt.common.storages.configuration import WithLocalFiles

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine, Dialect, Connection


@configspec(init=False)
class SqlalchemyCredentials(ConnectionStringCredentials):
    if TYPE_CHECKING:
        _engine: Optional["Engine"] = None

    engine_kwargs: Optional[Dict[str, Any]] = None
    """Additional keyword arguments passed to `sqlalchemy.create_engine`"""

    engine_args: Optional[Dict[str, Any]] = None
    """DEPRECATED: use engine_kwargs instead"""

    def __init__(
        self, connection_string: Optional[Union[str, Dict[str, Any], "Engine"]] = None
    ) -> None:
        super().__init__(connection_string)  # type: ignore[arg-type]

    def _resolve_engine_kwargs(self) -> Dict[str, Any]:
        if self.engine_kwargs and self.engine_args:
            raise ValueError(
                "Both engine_kwargs and engine_args were provided. Use engine_kwargs only."
            )

        if self.engine_args and not self.engine_kwargs:
            warnings.warn(
                "`engine_args` is deprecated; use `engine_kwargs` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return self.engine_args

        return self.engine_kwargs or {}

    def parse_native_representation(self, native_value: Any) -> None:
        from sqlalchemy.engine import Engine

        if isinstance(native_value, Engine):
            self.engine = native_value
            super().parse_native_representation(
                native_value.url.render_as_string(hide_password=False)
            )
        else:
            super().parse_native_representation(native_value)

    def borrow_conn(self) -> "Connection":
        if getattr(self, "_conn_owner", None) is False:
            return self.engine.connect()

        if not hasattr(self, "_conn_lock"):
            self._conn_lock = threading.Lock()

        # obtain a lock because we have refcount concurrency
        with self._conn_lock:
            engine_ = self.engine
            # track open connections to properly close it
            self._conn_borrows += 1

        try:
            return engine_.connect()
        except Exception:
            with self._conn_lock:
                self._conn_borrows -= 1
                if self._conn_borrows == 0 and self._conn_owner:
                    self._delete_conn()
            raise

    def return_conn(self, borrowed_conn: "Connection") -> None:
        if getattr(self, "_conn_owner", None) is False:
            borrowed_conn.close()
            return
        # close the borrowed conn
        with self._conn_lock:
            borrowed_conn.close()
            # close the main conn if the last borrowed conn was closed
            assert self._conn_borrows > 0, "Returning connection when borrows is 0"
            self._conn_borrows -= 1
            if self._conn_borrows == 0 and self._conn_owner:
                self._delete_conn()

    def _delete_conn(self) -> None:
        if self._conn_borrows > 0:
            warnings.warn(
                f"Disposing engine {self._engine.url} with {self._conn_borrows} open conns."
            )
        self._engine.dispose()
        delattr(self, "_engine")

    def __del__(self) -> None:
        if hasattr(self, "_engine") and self._conn_owner:
            self._delete_conn()

    @property
    def engine(self) -> Optional["Engine"]:
        import sqlalchemy as sa

        # get existing or open and set new engine
        engine_kwargs = self._resolve_engine_kwargs()
        self._engine = getattr(
            self,
            "_engine",
            None,
        )
        if self._engine is None:
            self._engine = sa.create_engine(
                self.to_url().render_as_string(hide_password=False), **engine_kwargs
            )
        # set as owner if not yet set
        self._conn_owner = getattr(self, "_conn_owner", True)
        self._conn_borrows = getattr(self, "_conn_borrows", 0)
        return self._engine

    @engine.setter
    def engine(self, value: "Engine") -> None:
        self._engine = value
        self._conn_owner = False
        self._conn_borrows = 0

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
                "`engine_args` is deprecated; use `engine_kwargs` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.engine_kwargs = self.engine_args

        if self.engine_kwargs and not self.credentials.engine_kwargs:
            self.credentials.engine_kwargs = self.engine_kwargs

        # resolve local file path for sqlite backends
        if self.credentials.drivername == "sqlite":
            db = self.credentials.database
            if db == ":memory:":
                pass
            elif not db or not os.path.isabs(db):
                self.credentials.database = os.path.normpath(
                    self.make_location(db or None, SQLITE_DB_NAME_PAT)
                )
