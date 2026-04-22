import dataclasses
from typing import Final, Optional

from dlt.common.typing import TSecretStrValue
from dlt.common.configuration import configspec

from dlt.destinations.impl.postgres.configuration import (
    PostgresCredentials,
    PostgresClientConfiguration,
)


@configspec(init=False)
class RedshiftCredentials(PostgresCredentials):
    port: int = 5439
    password: TSecretStrValue = None
    username: str = None
    host: str = None
    client_encoding: Optional[str] = "utf-8"


@configspec
class RedshiftClientConfiguration(PostgresClientConfiguration):
    destination_type: Final[str] = dataclasses.field(default="redshift", init=False, repr=False, compare=False)  # type: ignore
    credentials: RedshiftCredentials = None

    staging_iam_role: Optional[str] = None
    has_case_sensitive_identifiers: bool = False

    def physical_destination(self) -> str:
        """Returns host:port."""
        if self.credentials and self.credentials.host:
            port = self.credentials.port or 5439
            return f"{self.credentials.host}:{port}"
        return ""
