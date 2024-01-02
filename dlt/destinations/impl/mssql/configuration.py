from typing import Final, ClassVar, Any, List, Optional, TYPE_CHECKING
from sqlalchemy.engine import URL

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.utils import digest128
from dlt.common.typing import TSecretValue
from dlt.common.exceptions import SystemConfigurationException

from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration


SUPPORTED_DRIVERS = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]


@configspec
class MsSqlCredentials(ConnectionStringCredentials):
    drivername: Final[str] = "mssql"  # type: ignore
    password: TSecretValue
    host: str
    port: int = 1433
    connect_timeout: int = 15
    driver: str = None

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "connect_timeout"]

    def parse_native_representation(self, native_value: Any) -> None:
        # TODO: Support ODBC connection string or sqlalchemy URL
        super().parse_native_representation(native_value)
        if self.query is not None:
            self.query = {k.lower(): v for k, v in self.query.items()}  # Make case-insensitive.
        if "driver" in self.query and self.query.get("driver") not in SUPPORTED_DRIVERS:
            raise SystemConfigurationException(
                f"""The specified driver "{self.query.get('driver')}" is not supported."""
                f" Choose one of the supported drivers: {', '.join(SUPPORTED_DRIVERS)}."
            )
        self.driver = self.query.get("driver", self.driver)
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
        self.driver = self._get_driver()
        if not self.is_partial():
            self.resolve()

    def _get_driver(self) -> str:
        if self.driver:
            return self.driver
        # Pick a default driver if available
        import pyodbc

        available_drivers = pyodbc.drivers()
        for d in SUPPORTED_DRIVERS:
            if d in available_drivers:
                return d
        docs_url = "https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16"
        raise SystemConfigurationException(
            f"No supported ODBC driver found for MS SQL Server.  See {docs_url} for information on"
            f" how to install the '{SUPPORTED_DRIVERS[0]}' on your platform."
        )

    def to_odbc_dsn(self) -> str:
        params = {
            "DRIVER": self.driver,
            "SERVER": f"{self.host},{self.port}",
            "DATABASE": self.database,
            "UID": self.username,
            "PWD": self.password,
        }
        if self.query is not None:
            params.update({k.upper(): v for k, v in self.query.items()})
        return ";".join([f"{k}={v}" for k, v in params.items()])


@configspec
class MsSqlClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = "mssql"  # type: ignore
    credentials: MsSqlCredentials

    create_indexes: bool = False

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            credentials: Optional[MsSqlCredentials] = None,
            dataset_name: str = None,
            default_schema_name: Optional[str] = None,
            create_indexes: Optional[bool] = None,
            destination_name: str = None,
            environment: str = None,
        ) -> None: ...
