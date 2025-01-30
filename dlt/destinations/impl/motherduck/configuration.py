import os
import dataclasses
import sys
from typing import Any, ClassVar, Dict, Final, List, Optional

from dlt.version import __version__
from dlt.common.configuration import configspec
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.typing import TSecretStrValue
from dlt.common.utils import digest128

from dlt.destinations.impl.duckdb.configuration import DuckDbBaseCredentials

MOTHERDUCK_DRIVERNAME = "md"
MOTHERDUCK_USER_AGENT = f"dlt/{__version__}({sys.platform})"
MOTHERDUCK_DEFAULT_TOKEN_ENV = "motherduck_token"


@configspec(init=False)
class MotherDuckCredentials(DuckDbBaseCredentials):
    drivername: Final[str] = dataclasses.field(  # type: ignore
        default="md", init=False, repr=False, compare=False
    )
    username: str = "motherduck"
    password: TSecretStrValue = None
    database: str = "my_db"
    custom_user_agent: Optional[str] = MOTHERDUCK_USER_AGENT

    read_only: bool = False  # open database read/write

    __config_gen_annotations__: ClassVar[List[str]] = ["password", "database"]

    def _conn_str(self) -> str:
        _str = f"{MOTHERDUCK_DRIVERNAME}:{self.database}"
        if self.password:
            _str += f"?motherduck_token={self.password}"
        return _str

    def _token_to_password(self) -> None:
        if self.query:
            # backward compat
            if "token" in self.query:
                self.password = self.query.pop("token")
            if "motherduck_token" in self.query:
                self.password = self.query.pop("motherduck_token")

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

    def _has_default_token(self) -> bool:
        # TODO: implement default connection interface
        return MOTHERDUCK_DEFAULT_TOKEN_ENV in os.environ

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
