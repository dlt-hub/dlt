from typing import Final, ClassVar, Any, List, Optional
from sqlalchemy.engine import URL

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.utils import digest128
from dlt.common.typing import TSecretValue
from dlt.common.exceptions import SystemConfigurationException

from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration


@configspec
class MsSqlCredentials(ConnectionStringCredentials):
    drivername: Final[str] = "mssql"  # type: ignore
    password: TSecretValue
    host: str
    port: int = 1433
    connect_timeout: int = 15
    odbc_driver: str = None

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "connect_timeout"]

    def parse_native_representation(self, native_value: Any) -> None:
        # TODO: Support ODBC connection string or sqlalchemy URL
        super().parse_native_representation(native_value)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))
        if not self.is_partial():
            self.resolve()

    def on_resolved(self) -> None:
        self.database = self.database.lower()

    def to_url(self) -> URL:
        url = super().to_url()
        url.update_query_pairs([("connect_timeout", str(self.connect_timeout))])
        return url

    def on_partial(self) -> None:
        self.odbc_driver = self._get_odbc_driver()
        if not self.is_partial():
            self.resolve()

    def _get_odbc_driver(self) -> str:
        if self.odbc_driver:
            return self.odbc_driver
        # Pick a default driver if available
        supported_drivers = ['ODBC Driver 18 for SQL Server', 'ODBC Driver 17 for SQL Server']
        import pyodbc
        available_drivers = pyodbc.drivers()
        for driver in supported_drivers:
            if driver in available_drivers:
                return driver
        docs_url = "https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16"
        raise SystemConfigurationException(
            f"No supported ODBC driver found for MS SQL Server.  "
            f"See {docs_url} for information on how to install the '{supported_drivers[0]}' on your platform."
        )

    def to_odbc_dsn(self) -> str:
        params = {
            "DRIVER": self.odbc_driver,
            "SERVER": self.host,
            "PORT": self.port,
            "DATABASE": self.database,
            "UID": self.username,
            "PWD": self.password,
        }
        if self.query:
            params.update(self.query)
        return ";".join([f"{k}={v}" for k, v in params.items()])



@configspec
class MsSqlClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_name: Final[str] = "mssql"  # type: ignore
    credentials: MsSqlCredentials

    create_indexes: bool = False

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""
