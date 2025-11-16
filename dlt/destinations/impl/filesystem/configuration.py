import dataclasses

from typing import Final, Optional, Type, Dict, Any

from dlt.common import logger
from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination.client import (
    CredentialsConfiguration,
    DestinationClientStagingConfiguration,
)
from dlt.common.storages import FilesystemConfigurationWithLocalFiles

from dlt.destinations.impl.filesystem.typing import TCurrentDateTime, TExtraPlaceholders
from dlt.destinations.path_utils import check_layout, get_unused_placeholders


@configspec
class FilesystemDestinationClientConfiguration(FilesystemConfigurationWithLocalFiles, DestinationClientStagingConfiguration):  # type: ignore[misc]
    destination_type: Final[str] = dataclasses.field(  # type: ignore[misc]
        default="filesystem", init=False, repr=False, compare=False
    )
    current_datetime: Optional[TCurrentDateTime] = None
    extra_placeholders: Optional[TExtraPlaceholders] = None
    max_state_files: int = 100
    """Maximum number of pipeline state files to keep; 0 or negative value disables cleanup."""
    always_refresh_views: bool = False
    """Always refresh table scanner views by setting the newest table metadata or globbing table files"""

    # Iceberg catalog configuration
    iceberg_catalog_name: Optional[str] = "default"
    """Name of the Iceberg catalog to use. Corresponds to catalog name in .pyiceberg.yaml"""

    iceberg_catalog_type: Optional[str] = "sql"
    """Type of Iceberg catalog: 'sql', 'rest', 'glue', 'hive', etc."""

    iceberg_catalog_uri: Optional[str] = None
    """
    URI for SQL catalog (e.g., 'postgresql://...') or REST catalog endpoint.
    If not provided, defaults to in-memory SQLite for backward compatibility.
    """

    iceberg_catalog_config: Optional[Dict[str, Any]] = None
    """
    Optional dictionary with complete catalog configuration.
    If provided, will be used instead of loading from .pyiceberg.yaml.
    Example for REST catalog:
        {
            'type': 'rest',
            'uri': 'https://catalog.example.com',
            'warehouse': 'my_warehouse',
            'credential': 'token',
            'scope': 'PRINCIPAL_ROLE:ALL'
        }
    Example for SQL catalog:
        {
            'type': 'sql',
            'uri': 'postgresql://user:pass@localhost/catalog'
        }
    """

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        return super().resolve_credentials_type()

    def on_resolved(self) -> None:
        # Validate layout and show unused placeholders
        _, layout_placeholders = check_layout(self.layout, self.extra_placeholders)
        unused_placeholders = get_unused_placeholders(
            layout_placeholders, list((self.extra_placeholders or {}).keys())
        )
        if unused_placeholders:
            logger.info(f"Found unused layout placeholders: {', '.join(unused_placeholders)}")
