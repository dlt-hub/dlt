"""GeoPackage destination factory"""

from typing import Type

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.geopackage.configuration import (
    GeoPackageClientConfiguration,
    GeoPackageCredentials,
)


def geopackage(
    file_path: str = "dlt_data.gpkg",
    create_indexes: bool = True,
    enable_spatial_index: bool = True,
    destination_name: str = None,
    environment: str = None,
) -> Destination[GeoPackageClientConfiguration, "GeoPackageClient"]:
    """
    Configure GeoPackage destination for spatial data storage

    GeoPackage is an OGC standard for storing geospatial data in SQLite.

    Args:
        file_path: Path to GeoPackage file (will be created if doesn't exist)
        create_indexes: Whether to create attribute indexes
        enable_spatial_index: Whether to create spatial indexes for geometry columns
        destination_name: Name of the destination
        environment: Environment name

    Returns:
        Configured Destination object

    Example:
        ```python
        import dlt
        from dlt.destinations import geopackage

        pipeline = dlt.pipeline(
            pipeline_name='spatial_pipeline',
            destination=geopackage(file_path='output.gpkg'),
            dataset_name='spatial_data'
        )
        ```
    """

    return Destination.from_reference(
        "geopackage",
        destination_name=destination_name,
        environment=environment,
        credentials=GeoPackageCredentials(file_path=file_path),
        create_indexes=create_indexes,
        enable_spatial_index=enable_spatial_index,
    )


__all__ = ['geopackage']
