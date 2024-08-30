import dataclasses
from typing import ClassVar, Final, Optional, Any, Dict, List

from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration, configspec
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration


@configspec
class DatabricksCredentials(CredentialsConfiguration):
    catalog: str = None
    server_hostname: str = None
    http_path: str = None
    access_token: Optional[TSecretStrValue] = None
    http_headers: Optional[Dict[str, str]] = None
    session_configuration: Optional[Dict[str, Any]] = None
    """Dict of session parameters that will be passed to `databricks.sql.connect`"""
    connection_parameters: Optional[Dict[str, Any]] = None
    """Additional keyword arguments that are passed to `databricks.sql.connect`"""
    socket_timeout: Optional[int] = 180

    __config_gen_annotations__: ClassVar[List[str]] = [
        "server_hostname",
        "http_path",
        "catalog",
        "access_token",
    ]

    def to_connector_params(self) -> Dict[str, Any]:
        return dict(
            catalog=self.catalog,
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
            session_configuration=self.session_configuration or {},
            _socket_timeout=self.socket_timeout,
            **(self.connection_parameters or {}),
        )


@configspec
class DatabricksClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="databricks", init=False, repr=False, compare=False)  # type: ignore[misc]
    credentials: DatabricksCredentials = None
    staging_credentials_name: Optional[str] = None
    "If set, credentials with given name will be used in copy command"
    is_staging_external_location: bool = False
    """If true, the temporary credentials are not propagated to the COPY command"""

    def __str__(self) -> str:
        """Return displayable destination location"""
        if self.staging_config:
            return str(self.staging_config.credentials)
        else:
            return "[no staging set]"
