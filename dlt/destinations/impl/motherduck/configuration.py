from typing import Any, ClassVar, Final, List, TYPE_CHECKING, Optional

from dlt.common.configuration import configspec
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration
from dlt.common.exceptions import DestinationTerminalException
from dlt.common.typing import TSecretValue
from dlt.common.utils import digest128
from dlt.common.configuration.exceptions import ConfigurationValueError

from dlt.destinations.impl.duckdb.configuration import DuckDbBaseCredentials

MOTHERDUCK_DRIVERNAME = "md"


@configspec
class MotherDuckCredentials(DuckDbBaseCredentials):
    drivername: Final[str] = "md"  # type: ignore
    username: str = "motherduck"

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

    def on_resolved(self) -> None:
        self._token_to_password()
        if self.drivername == MOTHERDUCK_DRIVERNAME and not self.password:
            raise ConfigurationValueError(
                "Motherduck schema 'md' was specified without corresponding token or password. The"
                " required format of connection string is: md:///<database_name>?token=<token>"
            )


@configspec
class MotherDuckClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = "motherduck"  # type: ignore
    credentials: MotherDuckCredentials

    create_indexes: bool = (
        False  # should unique indexes be created, this slows loading down massively
    )

    def fingerprint(self) -> str:
        """Returns a fingerprint of user access token"""
        if self.credentials and self.credentials.password:
            return digest128(self.credentials.password)
        return ""

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            credentials: Optional[MotherDuckCredentials] = None,
            dataset_name: str = None,
            default_schema_name: Optional[str] = None,
            create_indexes: Optional[bool] = None,
            destination_name: str = None,
            environment: str = None,
        ) -> None: ...


class MotherduckLocalVersionNotSupported(DestinationTerminalException):
    def __init__(self, duckdb_version: str) -> None:
        self.duckdb_version = duckdb_version
        super().__init__(
            f"Looks like your local duckdb version ({duckdb_version}) is not supported by"
            " Motherduck"
        )
