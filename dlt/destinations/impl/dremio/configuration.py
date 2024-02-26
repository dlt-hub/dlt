from typing import Final, Optional, Any, Dict, ClassVar, List, TYPE_CHECKING

from sqlalchemy.engine import URL

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration
from dlt.common.typing import TSecretStrValue
from dlt.common.utils import digest128


@configspec
class DremioCredentials(ConnectionStringCredentials):
    drivername: Final[str] = "grpc"  # type: ignore[misc]
    username: str = None
    password: Optional[TSecretStrValue] = None
    host: str = None
    port: Optional[int] = 32010
    database: str = None
    data_source: str
    flatten: bool = False

    __config_gen_annotations__: ClassVar[List[str]] = ["password", "warehouse", "role"]

    def to_url(self) -> URL:
        return URL.create(drivername=self.drivername, host=self.host, port=self.port)

    def db_kwargs(self) -> Dict[str, Any]:
        return dict(username=self.username, password=self.password)


@configspec
class DremioClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = "dremio"  # type: ignore[misc]
    credentials: DremioCredentials
    staging_data_source: str
    """The name of the staging data source"""
    keep_staged_files: bool = True
    """Whether to keep or delete the staged files after COPY INTO succeeds"""

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
            credentials: DremioCredentials = None,
            dataset_name: str = None,
            default_schema_name: str = None,
            stage_name: str = None,
            keep_staged_files: bool = True,
            destination_name: str = None,
            environment: str = None,
        ) -> None: ...
