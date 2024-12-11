import dataclasses
from typing import ClassVar, Final, Optional, Any, Dict, List

from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration, configspec
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration
from dlt.common.configuration.exceptions import ConfigurationValueError


DATABRICKS_APPLICATION_ID = "dltHub_dlt"


@configspec
class DatabricksCredentials(CredentialsConfiguration):
    catalog: str = None
    server_hostname: str = None
    http_path: str = None
    access_token: Optional[TSecretStrValue] = None
    client_id: Optional[TSecretStrValue] = None
    client_secret: Optional[TSecretStrValue] = None
    http_headers: Optional[Dict[str, str]] = None
    session_configuration: Optional[Dict[str, Any]] = None
    """Dict of session parameters that will be passed to `databricks.sql.connect`"""
    connection_parameters: Optional[Dict[str, Any]] = None
    """Additional keyword arguments that are passed to `databricks.sql.connect`"""
    socket_timeout: Optional[int] = 180
    user_agent_entry: Optional[str] = DATABRICKS_APPLICATION_ID

    __config_gen_annotations__: ClassVar[List[str]] = [
        "server_hostname",
        "http_path",
        "catalog",
        "client_id",
        "client_secret",
        "access_token",
    ]

    def on_resolved(self) -> None:
        if not ((self.client_id and self.client_secret) or self.access_token):
            raise ConfigurationValueError(
                "No valid authentication method detected. Provide either 'client_id' and"
                " 'client_secret' for OAuth, or 'access_token' for token-based authentication."
            )

    def to_connector_params(self) -> Dict[str, Any]:
        conn_params = dict(
            catalog=self.catalog,
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
            session_configuration=self.session_configuration or {},
            _socket_timeout=self.socket_timeout,
            **(self.connection_parameters or {}),
        )

        if self.user_agent_entry:
            conn_params["_user_agent_entry"] = (
                conn_params.get("_user_agent_entry") or self.user_agent_entry
            )

        return conn_params


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
