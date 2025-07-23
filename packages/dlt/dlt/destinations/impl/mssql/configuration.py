import dataclasses
from typing import Final, ClassVar, Any, List, Dict

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.utils import digest128
from dlt.common.typing import TSecretStrValue
from dlt.common.exceptions import SystemConfigurationException

from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration


@configspec(init=False)
class MsSqlCredentials(ConnectionStringCredentials):
    drivername: Final[str] = dataclasses.field(default="mssql", init=False, repr=False, compare=False)  # type: ignore
    database: str = None
    username: str = None
    password: TSecretStrValue = None
    host: str = None
    port: int = 1433
    connect_timeout: int = 30
    driver: str = None

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "connect_timeout"]

    SUPPORTED_DRIVERS: ClassVar[List[str]] = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
    ]

    def parse_native_representation(self, native_value: Any) -> None:
        # TODO: Support ODBC connection string or sqlalchemy URL
        super().parse_native_representation(native_value)
        if self.query is not None:
            self.query = {k.lower(): v for k, v in self.query.items()}  # Make case-insensitive.
        self.driver = self.query.get("driver", self.driver)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))

    def on_resolved(self) -> None:
        if self.driver not in self.SUPPORTED_DRIVERS:
            raise SystemConfigurationException(
                f"The specified driver `{self.driver}` is not supported."
                f" Choose one of the supported drivers: {', '.join(self.SUPPORTED_DRIVERS)}."
            )
        self.database = self.database.lower()

    def get_query(self) -> Dict[str, Any]:
        query = dict(super().get_query())
        query["connect_timeout"] = self.connect_timeout
        return query

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
        for d in self.SUPPORTED_DRIVERS:
            if d in available_drivers:
                return d
        docs_url = "https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16"
        raise SystemConfigurationException(
            f"No supported ODBC driver found for MS SQL Server.  See {docs_url} for information on"
            f" how to install the `{self.SUPPORTED_DRIVERS[0]}` on your platform."
        )

    def _get_odbc_dsn_dict(self) -> Dict[str, Any]:
        params = {
            "DRIVER": self.driver,
            "SERVER": f"{self.host},{self.port}",
            "DATABASE": self.database,
            "UID": self.username,
            "PWD": self.password,
        }
        if self.query is not None:
            params.update({k.upper(): v for k, v in self.query.items()})
        return params

    def to_odbc_dsn(self) -> str:
        params = self._get_odbc_dsn_dict()
        return ";".join([f"{k}={v}" for k, v in params.items()])


@configspec
class MsSqlClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="mssql", init=False, repr=False, compare=False)  # type: ignore
    credentials: MsSqlCredentials = None

    create_indexes: bool = False
    has_case_sensitive_identifiers: bool = False

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""
