import dataclasses
import sys
from typing import Any, ClassVar, Dict, Final, List, Optional

from dlt.version import __version__
from dlt.common.configuration import configspec
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.typing import TSecretValue
from dlt.common.utils import digest128

from dlt.destinations.impl.duckdb.configuration import DuckDbBaseCredentials

MOTHERDUCK_DRIVERNAME = "md"
MOTHERDUCK_USER_AGENT = f"dlt/{__version__}({sys.platform})"


@configspec(init=False)
class MotherDuckCredentials(DuckDbBaseCredentials):
    drivername: Final[str] = dataclasses.field(  # type: ignore
        default="md", init=False, repr=False, compare=False
    )
    username: str = "motherduck"
    password: TSecretValue = None
    database: str = "my_db"
    custom_user_agent: Optional[str] = MOTHERDUCK_USER_AGENT

    read_only: bool = False  # open database read/write

    __config_gen_annotations__: ClassVar[List[str]] = ["password", "database"]

    def _conn_str(self) -> str:
        return f"{MOTHERDUCK_DRIVERNAME}:{self.database}?token={self.password}"

    def _token_to_password(self) -> None:
        # could be motherduck connection
        if self.query and "token" in self.query:
            self.password = TSecretValue(self.query.pop("token"))

    def borrow_conn(self, read_only: bool) -> Any:
        from duckdb import HTTPException, InvalidInputException

        try:
            return super().borrow_conn(read_only)
        except (InvalidInputException, HTTPException) as ext_ex:
            if "Failed to download extension" in str(ext_ex) and "motherduck" in str(ext_ex):
                from importlib.metadata import version as pkg_version

                raise MotherduckLocalVersionNotSupported(pkg_version("duckdb")) from ext_ex

            raise

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        self._token_to_password()

    def on_partial(self) -> None:
        """Takes a token from query string and reuses it as a password"""
        self._token_to_password()
        if not self.is_partial():
            self.resolve()

    def _get_conn_config(self) -> Dict[str, Any]:
        # If it was explicitly set to None/null then we
        # need to use the default value
        if self.custom_user_agent is None:
            self.custom_user_agent = MOTHERDUCK_USER_AGENT

        config = {}
        if self.custom_user_agent and self.custom_user_agent != "":
            config["custom_user_agent"] = self.custom_user_agent

        return config


@configspec
class MotherDuckClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore
        default="motherduck", init=False, repr=False, compare=False
    )
    credentials: MotherDuckCredentials = None

    create_indexes: bool = (
        False  # should unique indexes be created, this slows loading down massively
    )

    def fingerprint(self) -> str:
        """Returns a fingerprint of user access token"""
        if self.credentials and self.credentials.password:
            return digest128(self.credentials.password)
        return ""


class MotherduckLocalVersionNotSupported(DestinationTerminalException):
    def __init__(self, duckdb_version: str) -> None:
        self.duckdb_version = duckdb_version
        super().__init__(
            f"Looks like your local duckdb version ({duckdb_version}) is not supported by"
            " Motherduck"
        )
