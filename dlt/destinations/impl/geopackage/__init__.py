"""GeoPackage destination for spatial data

GeoPackage is an OGC standard SQLite extension for spatial data storage.
It provides a lightweight, portable format for geospatial data.

Example:
    ```python
    import dlt
    from dlt.sources.spatial import read_vector

    pipeline = dlt.pipeline(
        pipeline_name='spatial_etl',
        destination='geopackage',
        dataset_name='spatial.gpkg'
    )

    buildings = read_vector('/data/city.gdb', layer_name='Buildings')

    pipeline.run(buildings, table_name='buildings')
    ```
"""

from dlt.destinations.impl.geopackage.configuration import (
    GeoPackageClientConfiguration,
    GeoPackageCredentials,
)
from dlt.destinations.impl.geopackage.factory import geopackage

__all__ = ['GeoPackageClientConfiguration', 'GeoPackageCredentials', 'geopackage']
