from typing import ClassVar, Final, Optional, Any, Dict, List

from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration, configspec
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration


CATALOG_KEY_IN_SESSION_PROPERTIES = "databricks.catalog"


@configspec
class DatabricksCredentials(CredentialsConfiguration):
    catalog: Optional[str] = None  # type: ignore[assignment]
    schema: Optional[str] = None  # type: ignore[assignment]
    server_hostname: str = None
    http_path: str = None
    access_token: Optional[TSecretStrValue] = None
    client_id: Optional[str] = None
    client_secret: Optional[TSecretStrValue] = None
    session_properties: Optional[Dict[str, Any]] = None
    connection_parameters: Optional[Dict[str, Any]] = None
    auth_type: Optional[str] = None

    connect_retries: int = 1
    connect_timeout: Optional[int] = None
    retry_all: bool = False

    _credentials_provider: Optional[Dict[str, Any]] = None

    __config_gen_annotations__: ClassVar[List[str]] = [
        "server_hostname",
        "http_path",
        "catalog",
        "schema",
    ]

    def __post_init__(self) -> None:
        if "." in (self.schema or ""):
            raise ConfigurationValueError(
                f"The schema should not contain '.': {self.schema}\n"
                "If you are trying to set a catalog, please use `catalog` instead.\n"
            )

        session_properties = self.session_properties or {}
        if CATALOG_KEY_IN_SESSION_PROPERTIES in session_properties:
            if self.catalog is None:
                self.catalog = session_properties[CATALOG_KEY_IN_SESSION_PROPERTIES]
                del session_properties[CATALOG_KEY_IN_SESSION_PROPERTIES]
            else:
                raise ConfigurationValueError(
                    f"Got duplicate keys: (`{CATALOG_KEY_IN_SESSION_PROPERTIES}` "
                    'in session_properties) all map to "catalog"'
                )
        self.session_properties = session_properties

        if self.catalog is not None:
            catalog = self.catalog.strip()
            if not catalog:
                raise ConfigurationValueError(f"Invalid catalog name : `{self.catalog}`.")
            self.catalog = catalog
        else:
            self.catalog = "hive_metastore"

        connection_parameters = self.connection_parameters or {}
        for key in (
            "server_hostname",
            "http_path",
            "access_token",
            "client_id",
            "client_secret",
            "session_configuration",
            "catalog",
            "schema",
            "_user_agent_entry",
        ):
            if key in connection_parameters:
                raise ConfigurationValueError(f"The connection parameter `{key}` is reserved.")
        if "http_headers" in connection_parameters:
            http_headers = connection_parameters["http_headers"]
            if not isinstance(http_headers, dict) or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in http_headers.items()
            ):
                raise ConfigurationValueError(
                    "The connection parameter `http_headers` should be dict of strings: "
                    f"{http_headers}."
                )
        if "_socket_timeout" not in connection_parameters:
            connection_parameters["_socket_timeout"] = 180
        self.connection_parameters = connection_parameters

    def validate_creds(self) -> None:
        for key in ["host", "http_path"]:
            if not getattr(self, key):
                raise ConfigurationValueError(
                    "The config '{}' is required to connect to Databricks".format(key)
                )
        if not self.token and self.auth_type != "oauth":
            raise ConfigurationValueError(
                "The config `auth_type: oauth` is required when not using access token"
            )

        if not self.client_id and self.client_secret:
            raise ConfigurationValueError(
                "The config 'client_id' is required to connect "
                "to Databricks when 'client_secret' is present"
            )

    def to_connector_params(self) -> Dict[str, Any]:
        return dict(
            catalog=self.catalog,
            schema=self.schema,
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
            client_id=self.client_id,
            client_secret=self.client_secret,
            session_properties=self.session_properties or {},
            connection_parameters=self.connection_parameters or {},
            auth_type=self.auth_type,
        )


@configspec
class DatabricksClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_name: Final[str] = "databricks"  # type: ignore[misc]
    credentials: DatabricksCredentials

    stage_name: Optional[str] = None
    """Use an existing named stage instead of the default. Default uses the implicit table stage per table"""
    keep_staged_files: bool = True
    """Whether to keep or delete the staged files after COPY INTO succeeds"""

    def __str__(self) -> str:
        """Return displayable destination location"""
        if self.staging_config:
            return str(self.staging_config.credentials)
        else:
            return "[no staging set]"
