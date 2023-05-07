from typing import Final, ClassVar, Any, List
from sqlalchemy.engine import URL

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.destination.reference import DestinationClientDwhConfiguration


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

    def to_url(self) -> URL:
        url = super().to_url()
        url.update_query_pairs([("connect_timeout", str(self.connect_timeout))])
        return url


@configspec(init=True)
class PostgresClientConfiguration(DestinationClientDwhConfiguration):
    destination_name: Final[str] = "postgres"  # type: ignore
    credentials: PostgresCredentials

    create_indexes: bool = True
