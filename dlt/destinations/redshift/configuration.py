from typing import Final, Optional

from dlt.common.configuration import configspec
from dlt.common.typing import TSecretValue
from dlt.destinations.postgres.configuration import PostgresClientConfiguration, PostgresCredentials


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
    staging_iam_role: Optional[str] = None
