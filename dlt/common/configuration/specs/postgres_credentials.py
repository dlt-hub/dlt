from typing import Any, Dict, NamedTuple, Optional
from sqlalchemy.engine import URL, make_url

from dlt.common.typing import StrAny, TSecretValue
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration, configspec


@configspec
class ConnectionStringCredentials(CredentialsConfiguration):
    drivername: str = None
    database: str = None
    password: TSecretValue = None
    username: str = None
    host: str = None
    port: int = None
    query: Optional[Dict[str, str]] = None

    def from_native_representation(self, initial_value: Any) -> None:
        if not isinstance(initial_value, str):
            raise ValueError(initial_value)
        try:
            url = make_url(initial_value)
            self.update(url._asdict())
            # url.drivername = url.drivername
            # self.database = url.database
            # self.password = url.password
            # self.username = url.username
            # self.host = url.host
            # self.port = url.port
            # self.query = url.query
        except Exception:
            raise ValueError(initial_value)

    def check_integrity(self) -> None:
        self.database = self.database.lower()
        self.password = TSecretValue(self.password.strip())

    def to_native_representation(self) -> str:
        return self.to_url().render_as_string(hide_password=False)

    def to_url(self) -> URL:
        url = URL.create(self.drivername, self.username, self.password, self.host, self.port, self.database)
        if self.query:
            url.update_query_dict(self.query, append=False)
        return url


@configspec
class PostgresCredentials(ConnectionStringCredentials):
    drivername: str = "postgresql"
    port: int = 5439
    connect_timeout: int = 15

    def from_native_representation(self, initial_value: Any) -> None:
        super().from_native_representation(initial_value)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))

    def check_integrity(self) -> None:
        self.database = self.database.lower()
        self.password = TSecretValue(self.password.strip())

    def to_url(self) -> URL:
        url = super().to_url()
        url.update_query_pairs([("connect_timeout", str(self.connect_timeout))])
        return url
