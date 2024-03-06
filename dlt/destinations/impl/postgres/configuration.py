from typing import Final, ClassVar, Any, List, TYPE_CHECKING
from dlt.common.libs.sql_alchemy import URL

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.utils import digest128
from dlt.common.typing import TSecretValue

from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration


@configspec
class PostgresCredentials(ConnectionStringCredentials):
    drivername: Final[str] = "postgresql"  # type: ignore
    password: TSecretValue
    host: str
    port: int = 5432
    connect_timeout: int = 15

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "connect_timeout"]

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))
        if not self.is_partial():
            self.resolve()

    def to_url(self) -> URL:
        url = super().to_url()
        url.update_query_pairs([("connect_timeout", str(self.connect_timeout))])
        return url


@configspec
class PostgresClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = "postgres"  # type: ignore
    credentials: PostgresCredentials

    create_indexes: bool = True

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            credentials: PostgresCredentials = None,
            dataset_name: str = None,
            default_schema_name: str = None,
            create_indexes: bool = True,
            destination_name: str = None,
            environment: str = None,
        ) -> None: ...
