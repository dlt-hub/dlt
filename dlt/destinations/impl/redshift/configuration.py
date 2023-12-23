from typing import Final, Optional, TYPE_CHECKING

from dlt.common.typing import TSecretValue
from dlt.common.configuration import configspec
from dlt.common.utils import digest128

from dlt.destinations.impl.postgres.configuration import (
    PostgresCredentials,
    PostgresClientConfiguration,
)


@configspec
class RedshiftCredentials(PostgresCredentials):
    port: int = 5439
    password: TSecretValue = None
    username: str = None
    host: str = None


@configspec
class RedshiftClientConfiguration(PostgresClientConfiguration):
    destination_type: Final[str] = "redshift"  # type: ignore
    credentials: RedshiftCredentials
    staging_iam_role: Optional[str] = None

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            destination_type: str = None,
            credentials: PostgresCredentials = None,
            dataset_name: str = None,
            default_schema_name: str = None,
            staging_iam_role: str = None,
            destination_name: str = None,
            environment: str = None,
        ) -> None: ...
