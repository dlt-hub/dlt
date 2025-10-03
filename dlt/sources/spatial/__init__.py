"""
Spatial data source for dlt - supports OGR/GDAL formats for comprehensive spatial ETL

Created by: Baroudi Malek & Fawzi Hammami

This module provides spatial data extraction capabilities using OGR/GDAL, supporting:
- 170+ vector formats (Shapefile, FileGDB, GeoJSON, KML, DWG, DXF, etc.)
- 200+ raster formats (GeoTIFF, NetCDF, HDF5, ECW, etc.)
- Spatial transformations (reproject, buffer, clip, spatial filter)
- Integration with PostGIS, GeoPackage, and other spatial destinations

Example:
    ```python
    import dlt
    from dlt.sources.spatial import read_vector, reproject, buffer_geometry

    pipeline = dlt.pipeline(
        pipeline_name='spatial_etl',
        destination='postgres',
        dataset_name='spatial_data'
    )

    roads = read_vector(
        file_path='/data/city.gdb',
        layer_name='Roads',
        geometry_format='wkt'
    )

    roads_transformed = (
        roads
        | reproject(source_crs='EPSG:2154', target_crs='EPSG:4326')
        | buffer_geometry(distance=10)
    )

    pipeline.run(roads_transformed, table_name='roads_buffered')
    ```
"""

from typing import Any, Dict, Iterator, List, Optional, Union, Tuple, Literal

import dlt
from dlt.extract import decorators
from dlt.sources import DltResource

from dlt.sources.spatial.readers import (
    _read_vector,
    _read_raster,
    _read_vector_batch,
)
from dlt.sources.spatial.transformers import (
    _reproject,
    _buffer_geometry,
    _spatial_filter,
    _validate_geometry,
    _simplify_geometry,
    _spatial_join,
    _attribute_mapper,
    _geometry_extractor,
)
from dlt.sources.spatial.helpers import (
    SpatialFileSystem,
    detect_spatial_format,
    get_supported_formats,
)
from dlt.sources.spatial.settings import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_GEOMETRY_FORMAT,
    SUPPORTED_OGR_DRIVERS,
    SUPPORTED_GDAL_DRIVERS,
)


@decorators.resource(primary_key="feature_id")
def read_vector(
    file_path: str,
    layer_name: Optional[str] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    geometry_format: Literal['wkt', 'wkb', 'geojson', 'geometry'] = DEFAULT_GEOMETRY_FORMAT,
    target_crs: Optional[str] = None,
    attribute_filter: Optional[str] = None,
    spatial_filter: Optional[Union[str, Tuple[float, float, float, float]]] = None,
    **ogr_options: Any
) -> Iterator[List[Dict[str, Any]]]:
    """
    Read vector data using OGR (supports 170+ formats)

    Supported formats include:
    - ESRI: Shapefile, FileGDB, PersonalGDB, ArcSDE
    - CAD: DWG, DXF, DGN (MicroStation)
    - GIS: GeoPackage, GeoJSON, KML, GML, KMZ
    - Databases: PostGIS, Oracle Spatial, SQL Server Spatial, MySQL Spatial
    - Other: MapInfo, GPX, OSM, CSV with WKT

    Args:
        file_path (str): Path to spatial file or connection string
        layer_name (Optional[str]): Layer name to read (first layer if None)
        chunk_size (int): Number of features per batch (default: 1000)
        geometry_format (str): Format for geometry output: 'wkt', 'wkb', 'geojson', 'geometry'
        target_crs (Optional[str]): Target CRS for reprojection (e.g., 'EPSG:4326')
        attribute_filter (Optional[str]): SQL WHERE clause for filtering
        spatial_filter (Optional[Union[str, Tuple]]): WKT geometry or bbox (minx, miny, maxx, maxy)
        **ogr_options: Additional OGR open options

    Yields:
        List[Dict[str, Any]]: Batches of features with attributes and geometry

    Example:
        ```python
        # Read ESRI FileGDB
        buildings = read_vector(
            file_path='/data/city.gdb',
            layer_name='Buildings',
            target_crs='EPSG:4326',
            attribute_filter="height > 100"
        )
        ```
    """
    yield from _read_vector(
        file_path=file_path,
        layer_name=layer_name,
        chunk_size=chunk_size,
        geometry_format=geometry_format,
        target_crs=target_crs,
        attribute_filter=attribute_filter,
        spatial_filter=spatial_filter,
        **ogr_options
    )


@decorators.resource(primary_key="band_info")
def read_raster(
    file_path: str,
    bands: Optional[List[int]] = None,
    chunk_size: Optional[Tuple[int, int]] = None,
    window: Optional[Tuple[int, int, int, int]] = None,
    target_crs: Optional[str] = None,
    resample_method: Literal['nearest', 'bilinear', 'cubic', 'average'] = 'nearest',
    output_format: Literal['array', 'bytes', 'base64'] = 'bytes',
    **gdal_options: Any
) -> Iterator[Dict[str, Any]]:
    """
    Read raster data using GDAL (supports 200+ formats)

    Supported formats include:
    - Common: GeoTIFF, Cloud-Optimized GeoTIFF (COG), NetCDF, HDF5
    - Imagery: ECW, MrSID, JPEG2000, NITF
    - Digital Elevation: DEM, SRTM, ASTER
    - Point Clouds: LAS/LAZ (via PDAL)

    Args:
        file_path (str): Path to raster file
        bands (Optional[List[int]]): Band indices to read (all bands if None)
        chunk_size (Optional[Tuple[int, int]]): Size of chunks to read (width, height)
        window (Optional[Tuple[int, int, int, int]]): Window to read (xoff, yoff, xsize, ysize)
        target_crs (Optional[str]): Target CRS for warping
        resample_method (str): Resampling method for reprojection
        output_format (str): Format for raster data output
        **gdal_options: Additional GDAL open options

    Yields:
        Dict[str, Any]: Raster data with metadata

    Example:
        ```python
        # Read GeoTIFF elevation data
        elevation = read_raster(
            file_path='/data/dem.tif',
            bands=[1],
            target_crs='EPSG:3857',
            resample_method='cubic'
        )
        ```
    """
    yield from _read_raster(
        file_path=file_path,
        bands=bands,
        chunk_size=chunk_size,
        window=window,
        target_crs=target_crs,
        resample_method=resample_method,
        output_format=output_format,
        **gdal_options
    )


reproject = decorators.transformer()(_reproject)
buffer_geometry = decorators.transformer()(_buffer_geometry)
spatial_filter = decorators.transformer()(_spatial_filter)
validate_geometry = decorators.transformer()(_validate_geometry)
simplify_geometry = decorators.transformer()(_simplify_geometry)
spatial_join = decorators.transformer()(_spatial_join)
attribute_mapper = decorators.transformer()(_attribute_mapper)
geometry_extractor = decorators.transformer()(_geometry_extractor)


__all__ = [
    'read_vector',
    'read_raster',
    'reproject',
    'buffer_geometry',
    'spatial_filter',
    'validate_geometry',
    'simplify_geometry',
    'spatial_join',
    'attribute_mapper',
    'geometry_extractor',
    'detect_spatial_format',
    'get_supported_formats',
    'SpatialFileSystem',
    'DEFAULT_CHUNK_SIZE',
    'DEFAULT_GEOMETRY_FORMAT',
    'SUPPORTED_OGR_DRIVERS',
    'SUPPORTED_GDAL_DRIVERS',
]
