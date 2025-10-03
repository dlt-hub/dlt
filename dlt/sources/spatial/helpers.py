"""Helper functions and utilities for spatial data processing"""

from typing import Any, Dict, List, Optional, Tuple, Union
import os
from pathlib import Path

try:
    from osgeo import ogr, osr, gdal
    ogr.UseExceptions()
    osr.UseExceptions()
    gdal.UseExceptions()
    HAS_GDAL = True
except ImportError:
    HAS_GDAL = False

from dlt.common.exceptions import MissingDependencyException

if not HAS_GDAL:
    raise MissingDependencyException(
        "Spatial sources",
        ["gdal"],
        "Install GDAL library using: pip install 'dlt[spatial]' or pip install GDAL"
    )


class SpatialFileSystem:
    """Helper class for working with spatial file systems and connections"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self._datasource = None

    def open(self) -> Any:
        """Open the spatial datasource"""
        if not self._datasource:
            self._datasource = ogr.Open(self.connection_string)
            if not self._datasource:
                raise ValueError(f"Cannot open spatial datasource: {self.connection_string}")
        return self._datasource

    def close(self) -> None:
        """Close the spatial datasource"""
        if self._datasource:
            self._datasource = None

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def detect_spatial_format(file_path: str) -> str:
    """
    Auto-detect spatial format using OGR/GDAL drivers

    Args:
        file_path (str): Path to spatial file

    Returns:
        str: Detected driver name

    Raises:
        ValueError: If format cannot be detected
    """
    priority_drivers = [
        'OpenFileGDB',
        'FileGDB',
        'ESRI Shapefile',
        'GPKG',
        'GeoJSON',
        'KML',
        'DXF',
        'DWG',
        'MapInfo File',
    ]

    for driver_name in priority_drivers:
        driver = ogr.GetDriverByName(driver_name)
        if driver:
            ds = driver.Open(file_path)
            if ds:
                ds = None
                return driver_name

    for i in range(ogr.GetDriverCount()):
        driver = ogr.GetDriver(i)
        ds = driver.Open(file_path)
        if ds:
            ds = None
            return driver.GetName()

    raise ValueError(f"Cannot detect spatial format for: {file_path}")


def get_supported_formats() -> Dict[str, List[str]]:
    """
    Get all supported OGR and GDAL formats

    Returns:
        Dict with 'vector' and 'raster' format lists
    """
    vector_formats = []
    for i in range(ogr.GetDriverCount()):
        driver = ogr.GetDriver(i)
        vector_formats.append({
            'name': driver.GetName(),
            'long_name': driver.GetDescription(),
        })

    raster_formats = []
    for i in range(gdal.GetDriverCount()):
        driver = gdal.GetDriver(i)
        raster_formats.append({
            'name': driver.ShortName,
            'long_name': driver.LongName,
        })

    return {
        'vector': vector_formats,
        'raster': raster_formats,
    }


def create_coordinate_transformation(
    source_crs: Union[str, int, osr.SpatialReference],
    target_crs: Union[str, int, osr.SpatialReference]
) -> osr.CoordinateTransformation:
    """
    Create coordinate transformation between two CRS

    Args:
        source_crs: Source CRS (EPSG code, WKT, or SpatialReference)
        target_crs: Target CRS (EPSG code, WKT, or SpatialReference)

    Returns:
        CoordinateTransformation object
    """
    source_srs = _parse_crs(source_crs)
    target_srs = _parse_crs(target_crs)

    return osr.CoordinateTransformation(source_srs, target_srs)


def _parse_crs(crs: Union[str, int, osr.SpatialReference]) -> osr.SpatialReference:
    """Parse CRS input into SpatialReference object"""
    if isinstance(crs, osr.SpatialReference):
        return crs

    srs = osr.SpatialReference()

    if isinstance(crs, int):
        srs.ImportFromEPSG(crs)
    elif isinstance(crs, str):
        if crs.startswith('EPSG:'):
            epsg_code = int(crs.split(':')[1])
            srs.ImportFromEPSG(epsg_code)
        else:
            srs.SetFromUserInput(crs)
    else:
        raise ValueError(f"Invalid CRS format: {crs}")

    return srs


def get_geometry_type_name(geom_type: int) -> str:
    """Get geometry type name from OGR geometry type code"""
    type_mapping = {
        1: 'Point',
        2: 'LineString',
        3: 'Polygon',
        4: 'MultiPoint',
        5: 'MultiLineString',
        6: 'MultiPolygon',
        7: 'GeometryCollection',
        1001: 'Point Z',
        1002: 'LineString Z',
        1003: 'Polygon Z',
        1004: 'MultiPoint Z',
        1005: 'MultiLineString Z',
        1006: 'MultiPolygon Z',
        2001: 'Point M',
        2002: 'LineString M',
        2003: 'Polygon M',
        3001: 'Point ZM',
        3002: 'LineString ZM',
        3003: 'Polygon ZM',
    }
    return type_mapping.get(geom_type, f'Unknown ({geom_type})')


def extract_extent(layer: Any) -> Optional[Tuple[float, float, float, float]]:
    """
    Extract spatial extent (bounding box) from layer

    Returns:
        Tuple of (minx, miny, maxx, maxy) or None
    """
    try:
        extent = layer.GetExtent()
        return extent
    except Exception:
        return None


def get_layer_info(datasource: Any, layer_index: int = 0) -> Dict[str, Any]:
    """
    Get comprehensive information about a layer

    Args:
        datasource: OGR datasource
        layer_index: Index of layer to inspect

    Returns:
        Dictionary with layer metadata
    """
    layer = datasource.GetLayer(layer_index)

    layer_defn = layer.GetLayerDefn()
    field_info = []

    for i in range(layer_defn.GetFieldCount()):
        field = layer_defn.GetFieldDefn(i)
        field_info.append({
            'name': field.GetName(),
            'type': field.GetTypeName(),
            'width': field.GetWidth(),
            'precision': field.GetPrecision(),
        })

    return {
        'name': layer.GetName(),
        'feature_count': layer.GetFeatureCount(),
        'geometry_type': get_geometry_type_name(layer.GetGeomType()),
        'spatial_reference': layer.GetSpatialRef().ExportToWkt() if layer.GetSpatialRef() else None,
        'extent': extract_extent(layer),
        'fields': field_info,
    }


def list_layers(file_path: str) -> List[Dict[str, Any]]:
    """
    List all layers in a spatial datasource

    Args:
        file_path: Path to spatial file

    Returns:
        List of layer information dictionaries
    """
    datasource = ogr.Open(file_path)
    if not datasource:
        raise ValueError(f"Cannot open datasource: {file_path}")

    layers = []
    for i in range(datasource.GetLayerCount()):
        try:
            layer_info = get_layer_info(datasource, i)
            layers.append(layer_info)
        except Exception as e:
            layers.append({
                'name': f'Layer {i}',
                'error': str(e)
            })

    datasource = None
    return layers


def validate_ogr_sql(sql: str) -> bool:
    """
    Validate OGR SQL syntax (basic validation)

    Args:
        sql: SQL string to validate

    Returns:
        True if valid, False otherwise
    """
    forbidden_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE']
    sql_upper = sql.upper()

    for keyword in forbidden_keywords:
        if keyword in sql_upper:
            return False

    return True
