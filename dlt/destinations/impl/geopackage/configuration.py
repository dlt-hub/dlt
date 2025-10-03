"""GeoPackage destination configuration"""

from typing import Final, Optional, ClassVar
from pathlib import Path

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.destination.reference import DestinationClientDwhConfiguration


@configspec
class GeoPackageCredentials(ConnectionStringCredentials):
    """Credentials for GeoPackage file"""

    drivername: Final[str] = "geopackage"
    file_path: str = None

    def __init__(self, file_path: str = ":memory:", **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path

    def to_native_representation(self) -> str:
        """Return file path for OGR connection"""
        return self.file_path

    def parse_native_representation(self, native_value: str) -> None:
        """Parse file path from connection string"""
        self.file_path = native_value


@configspec
class GeoPackageClientConfiguration(DestinationClientDwhConfiguration):
    """Configuration for GeoPackage destination client"""

    destination_type: Final[str] = "geopackage"
    credentials: GeoPackageCredentials = None
    create_indexes: bool = True
    enable_spatial_index: bool = True
    application_id: Optional[int] = None

    def __init__(
        self,
        file_path: str = "dlt_data.gpkg",
        create_indexes: bool = True,
        enable_spatial_index: bool = True,
        credentials: Optional[GeoPackageCredentials] = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs
    ):
        super().__init__(
            credentials=credentials or GeoPackageCredentials(file_path),
            destination_name=destination_name,
            environment=environment,
            **kwargs
        )
        self.create_indexes = create_indexes
        self.enable_spatial_index = enable_spatial_index

    def fingerprint(self) -> str:
        """Returns fingerprint of file path"""
        if self.credentials:
            return self.credentials.file_path
        return ""
