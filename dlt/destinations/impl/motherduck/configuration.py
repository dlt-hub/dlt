import os
import dataclasses
import sys
from urllib.parse import urlencode
from typing import Any, ClassVar, Dict, Final, List, Optional, TYPE_CHECKING

from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials
from dlt.version import __version__
from dlt.common.configuration import configspec
from dlt.common.configuration.specs.exceptions import NativeValueError
from dlt.common.destination.attach import TAttachType
from dlt.common.destination.client import (
    DestinationClientConfiguration,
    DestinationClientDwhWithStagingConfiguration,
    WithAttachableEngine,
)
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.utils import digest128
from dlt.common.typing import TSecretStrValue

from dlt.destinations.impl.duckdb.configuration import DuckDbBaseCredentials, DuckDbConnectionPool

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
else:
    DuckDBPyConnection = Any  # type: ignore[assignment,misc]

MOTHERDUCK_DRIVERNAME = "md"
MOTHERDUCK_USER_AGENT = f"dlt/{__version__}({sys.platform})"
MOTHERDUCK_DEFAULT_TOKEN_ENV = "motherduck_token"


class MotherduckConnectionPool(DuckDbConnectionPool):
    def borrow_conn(
        self,
        global_config: Dict[str, Any] = None,
        local_config: Dict[str, Any] = None,
        pragmas: List[str] = None,
    ) -> DuckDBPyConnection:
        from duckdb import HTTPException, InvalidInputException

        try:
            return super().borrow_conn(global_config, local_config, pragmas)
        except (InvalidInputException, HTTPException) as ext_ex:
            if "Failed to download extension" in str(ext_ex) and "motherduck" in str(ext_ex):
                from importlib.metadata import version as pkg_version

                raise MotherduckLocalVersionNotSupported(pkg_version("duckdb")) from ext_ex

            raise


@configspec(init=False)
class MotherDuckCredentials(DuckDbBaseCredentials, ConnectionStringCredentials):
    drivername: Final[str] = dataclasses.field(  # type: ignore
        default="md", init=False, repr=False, compare=False
    )
    username: str = "motherduck"
    password: TSecretStrValue = None
    database: str = "my_db"
    custom_user_agent: str = MOTHERDUCK_USER_AGENT

    __config_gen_annotations__: ClassVar[List[str]] = ["password", "database"]

    def _conn_str(self) -> str:
        # TODO: fix dbt profile.yml to receive full conn str
        _str = f"{MOTHERDUCK_DRIVERNAME}:{self.database}"

        q_ = dict(self.query or {})
        if self.password:
            q_["motherduck_token"] = self.password

        return _str + "?" + urlencode(q_)

    def _token_to_password(self) -> None:
        if self.query:
            # backward compat
            if "token" in self.query:
                self.password = self.query.pop("token")
            if "motherduck_token" in self.query:
                self.password = self.query.pop("motherduck_token")

    def parse_native_representation(self, native_value: Any) -> None:
        if isinstance(native_value, str):
            # https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#storing-the-access-token-as-an-environment-variable
            # ie. md:dlt_data_3?motherduck_token=<my service token>
            if native_value.startswith("md:") and not native_value.startswith("md:/"):
                native_value = "md:///" + native_value[3:]  # skip md:
        super().parse_native_representation(native_value)
        self._token_to_password()

    def on_partial(self) -> None:
        """Takes a token from query string and reuses it as a password"""
        self._token_to_password()
        if not self.is_partial() or self._has_default_token():
            self.resolve()

    def on_resolved(self) -> None:
        """Adds custom agent to global config"""
        if self.database == "":
            raise MotherDuckCatalogMissing(
                self.__class__,
                "md:",
                "MotherDuck connection string must include a catalog/database name, for example"
                " `md:my_db`.",
            )
        if self.global_config is None:
            self.global_config = {}
        self.global_config["custom_user_agent"] = self.custom_user_agent or MOTHERDUCK_USER_AGENT
        self.conn_pool = MotherduckConnectionPool(self)

    def _has_default_token(self) -> bool:
        # TODO: implement default connection interface
        return (
            MOTHERDUCK_DEFAULT_TOKEN_ENV in os.environ
            or MOTHERDUCK_DEFAULT_TOKEN_ENV.upper() in os.environ
        )


@configspec
class MotherDuckClientConfiguration(
    WithAttachableEngine, DestinationClientDwhWithStagingConfiguration
):
    destination_type: Final[str] = dataclasses.field(  # type: ignore
        default="motherduck", init=False, repr=False, compare=False
    )
    credentials: MotherDuckCredentials = None

    create_indexes: bool = (
        False  # should unique indexes be created, this slows loading down massively
    )

    def data_location(self) -> str:
        """Returns the account. One query engine accesses every database of that account. The
        token is the only account identity, so this method digests it."""
        if not (token := self.fingerprint()):
            self._no_data_location("dlt found no MotherDuck access token")
        return f"md://{token}"

    def fingerprint(self) -> str:
        """Returns a fingerprint of user access token."""
        if token := self._access_token():
            return digest128(token)
        return ""

    def _access_token(self) -> Optional[str]:
        """Returns the token that the connection uses to authenticate. `on_partial` accepts this
        token from the environment and from the credentials."""
        if self.credentials and self.credentials.password:
            return self.credentials.password
        return os.environ.get(MOTHERDUCK_DEFAULT_TOKEN_ENV) or os.environ.get(
            MOTHERDUCK_DEFAULT_TOKEN_ENV.upper()
        )

    def needs_attach(self, other: DestinationClientConfiguration) -> bool:
        """Returns False within one account. The query engine accesses every database of that
        account."""
        return not self.is_same_location(other)

    def attach_type(self) -> Optional[TAttachType]:
        return "motherduck"

    def can_attach(self, attach_type: TAttachType) -> bool:
        """Returns True only for plain duckdb. A MotherDuck query engine cannot attach another
        MotherDuck database, because the client must set the token before it opens the
        connection."""
        return attach_type == "duckdb"


class MotherDuckCatalogMissing(NativeValueError):
    pass


class MotherduckLocalVersionNotSupported(DestinationTerminalException):
    def __init__(self, duckdb_version: str) -> None:
        self.duckdb_version = duckdb_version
        super().__init__(
            f"Looks like your local duckdb version ({duckdb_version}) is not supported by"
            " Motherduck"
        )
