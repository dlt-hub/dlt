import dataclasses
from typing import ClassVar, Final, Optional, Any, Dict, List, List, Dict, cast, Callable

from dlt.common import logger
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration, configspec
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.utils import digest128

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
                # attempt context authentication
                from databricks.sdk import WorkspaceClient

                w = WorkspaceClient()
                self.access_token = w.dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)  # type: ignore[union-attr]
            except Exception:
                self.access_token = None

            try:
                from databricks.sdk import WorkspaceClient

                w = WorkspaceClient()
                self.access_token = w.config.authenticate  # type: ignore[assignment]
                logger.info(f"Will attempt to use default auth of type {w.config.auth_type}")
            except Exception:
                pass

            if not self.access_token:
                raise ConfigurationValueError(
                    "Authentication failed: No valid authentication method detected. "
                    "Provide either `client_id` and `client_secret` for OAuth authentication, "
                    "or `access_token` for token-based authentication."
                )

        if not self.server_hostname or not self.http_path:
            try:
                # attempt to fetch warehouse details
                from databricks.sdk import WorkspaceClient

                w = WorkspaceClient()
                # warehouse ID may be present in an env variable
                if w.config.warehouse_id:
                    warehouse = w.warehouses.get(w.config.warehouse_id)
                else:
                    # for some reason list of warehouses has different type than a single one 🤯
                    warehouse = list(w.warehouses.list())[0]  # type: ignore[assignment]
                logger.info(
                    f"Will attempt to use warehouse {warehouse.id} to get sql connection params"
                )
                self.server_hostname = self.server_hostname or warehouse.odbc_params.hostname
                self.http_path = self.http_path or warehouse.odbc_params.path
            except Exception:
                pass

        for param in ("catalog", "server_hostname", "http_path"):
            if not getattr(self, param):
                raise ConfigurationValueError(
                    f"Configuration error: Missing required parameter `{param}`. "
                    "Please provide it in the configuration."
                )

    def _get_oauth_credentials(self) -> Optional[Callable[[], Dict[str, str]]]:
        from databricks.sdk.core import Config, oauth_service_principal

        config = Config(
            host=f"https://{self.server_hostname}",
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return cast(Callable[[], Dict[str, str]], oauth_service_principal(config))

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

        if self.client_id and self.client_secret:
            conn_params["credentials_provider"] = self._get_oauth_credentials
        elif callable(self.access_token):
            # this is w.config.authenticator
            conn_params["credentials_provider"] = lambda: self.access_token
        else:
            # this is access token
            conn_params["access_token"] = self.access_token
        return conn_params

    def __str__(self) -> str:
        return f"databricks://{self.server_hostname}{self.http_path}/{self.catalog}"


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
    keep_staged_files: Optional[bool] = True
    """Tells if to keep the files in internal (volume) stage"""

    """Whether PRIMARY KEY or FOREIGN KEY constrains should be created"""
    create_indexes: bool = False

    def __str__(self) -> str:
        """Return displayable destination location"""
        if self.credentials:
            return str(self.credentials)
        else:
            return ""

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""
        if self.credentials and self.credentials.server_hostname:
            return digest128(self.credentials.server_hostname)
        return ""
