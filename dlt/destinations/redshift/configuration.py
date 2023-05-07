from typing import Final

from dlt.common.typing import TSecretValue
from dlt.common.configuration import configspec

from dlt.destinations.postgres.configuration import PostgresCredentials, PostgresClientConfiguration


@configspec
class RedshiftCredentials(PostgresCredentials):
    port: int = 5439
    password: TSecretValue = None
    username: str = None
    host: str = None


@configspec(init=True)
class RedshiftClientConfiguration(PostgresClientConfiguration):
    destination_name: Final[str] = "redshift"  # type: ignore
    credentials: RedshiftCredentials
