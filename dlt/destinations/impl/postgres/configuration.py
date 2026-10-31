import dataclasses
from typing import Dict, Final, ClassVar, Any, List, Optional

from dlt.common.destination.configuration import CsvFormatConfiguration
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.typing import TSecretStrValue
from dlt.common.utils import digest128

from dlt.common.destination.client import (
    DestinationClientConfiguration,
    DestinationClientDwhWithStagingConfiguration,
)


@configspec(init=False)
class PostgresCredentials(ConnectionStringCredentials):
    drivername: Final[str] = dataclasses.field(default="postgresql", init=False, repr=False, compare=False)  # type: ignore[misc]
    database: str = None
    username: str = None
    password: TSecretStrValue = None
    host: str = None
    port: int = 5432
    connect_timeout: int = 15
    client_encoding: Optional[str] = None
    session_timezone: Optional[str] = None
    """Timezone set on each connection. `None` keeps the server default"""

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "connect_timeout"]

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))
        self.client_encoding = self.query.get("client_encoding", self.client_encoding)

    def get_query(self) -> Dict[str, Any]:
        query = dict(super().get_query())
        query["connect_timeout"] = self.connect_timeout
        if self.client_encoding:
            query["client_encoding"] = self.client_encoding
        options = query.get("options", "")
        # `RESET ALL` and rollbacks revert a `SET` statement but not a startup option
        if self.session_timezone and "timezone=" not in options:
            query["options"] = f"{options} -ctimezone={self.session_timezone}".strip()
        return query


@configspec
class PostgresClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="postgres", init=False, repr=False, compare=False)  # type: ignore[misc]
    credentials: PostgresCredentials = None
    DEFAULT_PORT: ClassVar[int] = 5432

    create_indexes: bool = True

    csv_format: Optional[CsvFormatConfiguration] = None
    """Optional csv format configuration"""

    def fingerprint(self) -> str:
        """Returns a fingerprint of the configured host."""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""

    def data_location(self) -> str:
        """Returns host:port and the database to which a query engine binds."""
        if not self.credentials or not self.credentials.host:
            self._no_data_location("the configuration has no host")
        if not self.credentials.database:
            self._no_data_location("the configuration has no database")
        port = self.credentials.port or self.DEFAULT_PORT
        return f"{self.credentials.host}:{port}/{self.credentials.database}"
