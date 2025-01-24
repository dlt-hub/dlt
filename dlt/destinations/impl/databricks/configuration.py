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
    server_hostname: Optional[str] = None
    http_path: Optional[str] = None
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
            try:
                # attempt notebook context authentication
                from databricks.sdk import WorkspaceClient
                from databricks.sdk.service.sql import EndpointInfo

                w = WorkspaceClient()
                self.access_token = w.dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)  # type: ignore[union-attr]

                # pick the first warehouse on the list
                warehouses: List[EndpointInfo] = list(w.warehouses.list())
                self.server_hostname = warehouses[0].odbc_params.hostname
                self.http_path = warehouses[0].odbc_params.path
            except Exception:
                pass

            if not self.access_token:
                raise ConfigurationValueError(
                    "Databricks authentication failed: No valid authentication method detected."
                    " Please provide either 'client_id' and 'client_secret' for OAuth, or"
                    " 'access_token' for token-based authentication."
                )

        if not self.server_hostname or not self.http_path or not self.catalog:
            raise ConfigurationValueError(
                "Databricks authentication failed: 'server_hostname', 'http_path', and 'catalog'"
                " are required parameters. Ensure all are provided."
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
    staging_volume_name: Optional[str] = None
    """Name of the Databricks managed volume for temporary storage, e.g., <catalog_name>.<database_name>.<volume_name>. Defaults to '_dlt_temp_load_volume' if not set."""

    def on_resolved(self) -> None:
        if self.staging_volume_name and self.staging_volume_name.count(".") != 2:
            raise ConfigurationValueError(
                f"Invalid staging_volume_name format: {self.staging_volume_name}. Expected format"
                " is '<catalog_name>.<database_name>.<volume_name>'."
            )

    def __str__(self) -> str:
        """Return displayable destination location"""
        if self.staging_config:
            return str(self.staging_config.credentials)
        else:
            return "[no staging set]"
