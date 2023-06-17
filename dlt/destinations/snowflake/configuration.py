from typing import Final, Optional, Any

from sqlalchemy.engine import URL

from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.configuration import configspec
from dlt.common.destination.reference import DestinationClientDwhConfiguration


@configspec
class SnowflakeCredentials(ConnectionStringCredentials):
    drivername: Final[str] = "snowflake"  # type: ignore[misc]
    password: TSecretStrValue = None
    host: str = None
    database: str = None
    warehouse: Optional[str] = None
    role: Optional[str] = None

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        if 'warehouse' in self.query:
            self.warehouse = self.query['warehouse']
        if 'role' in self.query:
            self.role = self.query['role']

    def to_url(self) -> URL:
        query = dict(self.query or {})
        if self.warehouse and 'warehouse' not in query:
            query['warehouse'] = self.warehouse
        if self.role and 'role' not in query:
            query['role'] = self.role
        return URL.create(self.drivername, self.username, self.password, self.host, self.port, self.database, query)


@configspec(init=True)
class SnowflakeClientConfiguration(DestinationClientDwhConfiguration):
    destination_name: Final[str] = "snowflake"  # type: ignore[misc]
    credentials: SnowflakeCredentials
