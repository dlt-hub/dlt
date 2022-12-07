from typing import Any, ClassVar, Dict, Final, List, Optional
from sqlalchemy.engine import URL, make_url
from dlt.common.configuration.specs.exceptions import InvalidConnectionString

from dlt.common.typing import TSecretValue
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

    __config_gen_annotations__: ClassVar[List[str]] = ["port"]

    def parse_native_representation(self, native_value: Any) -> None:
        if not isinstance(native_value, str):
            raise InvalidConnectionString(self.__class__, native_value)
        try:
            url = make_url(native_value)
            self.update(url._asdict())
            if self.query is not None:
                self.query = dict(self.query)
            self.__is_resolved__ = not self.is_partial()
        except Exception:
            raise InvalidConnectionString(self.__class__, native_value)

    def on_resolved(self) -> None:
        self.database = self.database.lower()
        self.password = TSecretValue(self.password.strip())

    def to_native_representation(self) -> str:
        return self.to_url().render_as_string(hide_password=False)

    def to_url(self) -> URL:
        return URL.create(self.drivername, self.username, self.password, self.host, self.port, self.database, self.query)


@configspec
class PostgresCredentials(ConnectionStringCredentials):
    drivername: Final[str] = "postgresql"  # type: ignore
    port: int = 5432
    connect_timeout: int = 15

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "connect_timeout"]

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))

    def on_resolved(self) -> None:
        self.database = self.database.lower()
        self.password = TSecretValue(self.password.strip())

    def to_url(self) -> URL:
        url = super().to_url()
        url.update_query_pairs([("connect_timeout", str(self.connect_timeout))])
        return url

    def __str__(self) -> str:
        return self.to_url().render_as_string(hide_password=True)


@configspec
class RedshiftCredentials(PostgresCredentials):
    port: int = 5439
