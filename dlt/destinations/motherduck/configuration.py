from typing import Any, ClassVar, Final, List

from dlt.common.configuration import configspec
from dlt.common.destination.reference import DestinationClientDwhConfiguration
from dlt.common.typing import TSecretValue
from dlt.common.configuration.exceptions import ConfigurationValueError

from dlt.destinations.duckdb.configuration import DuckDbBaseCredentials

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

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        self._token_to_password()

    def on_resolved(self) -> None:
        self._token_to_password()
        if self.drivername == MOTHERDUCK_DRIVERNAME and not self.password:
            raise ConfigurationValueError("Motherduck schema 'md' was specified without corresponding token or password. The required format of connection string is: md:///<database_name>?token=<token>")


@configspec(init=True)
class MotherDuckClientConfiguration(DestinationClientDwhConfiguration):
    destination_name: Final[str] = "motherduck"  # type: ignore
    credentials: MotherDuckCredentials

    create_indexes: bool = False  # should unique indexes be created, this slows loading down massively
