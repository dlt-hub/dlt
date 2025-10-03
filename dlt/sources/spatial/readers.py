"""OGR/GDAL readers for vector and raster spatial data"""

from typing import Any, Dict, Iterator, List, Optional, Tuple, Union, Literal
import base64

try:
    from osgeo import ogr, osr, gdal
    import numpy as np
    ogr.UseExceptions()
    osr.UseExceptions()
    gdal.UseExceptions()
    HAS_GDAL = True
except ImportError:
    HAS_GDAL = False

from dlt.common.exceptions import MissingDependencyException
from dlt.sources.spatial.helpers import (
    create_coordinate_transformation,
    detect_spatial_format,
    validate_ogr_sql,
)
from dlt.sources.spatial.settings import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_RASTER_CHUNK_SIZE,
    OGR_FIELD_TYPE_MAP,
)

if not HAS_GDAL:
    raise MissingDependencyException(
        "Spatial sources",
        ["gdal", "numpy"],
        "Install GDAL and numpy: pip install 'dlt[spatial]'"
    )


def _read_vector(
    file_path: str,
    layer_name: Optional[str] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    geometry_format: Literal['wkt', 'wkb', 'geojson', 'geometry'] = 'wkt',
    target_crs: Optional[str] = None,
    attribute_filter: Optional[str] = None,
    spatial_filter: Optional[Union[str, Tuple[float, float, float, float]]] = None,
    **ogr_options: Any
) -> Iterator[List[Dict[str, Any]]]:
    """
    Internal function to read vector data using OGR

    Supports 170+ vector formats including:
    - ESRI Shapefile, FileGDB, PersonalGDB
    - CAD: DWG, DXF, DGN
    - GeoPackage, GeoJSON, KML, GML
    - PostGIS, Oracle Spatial, SQL Server Spatial
    """
    format_name = detect_spatial_format(file_path)
    driver = ogr.GetDriverByName(format_name)

    if not driver:
        raise ValueError(f"Cannot load driver for format: {format_name}")

    open_options = []
    for key, value in ogr_options.items():
        open_options.append(f"{key}={value}")

    datasource = driver.Open(file_path, 0, open_options)
    if not datasource:
        raise ValueError(f"Cannot open spatial file: {file_path}")

    if layer_name:
        layer = datasource.GetLayerByName(layer_name)
        if not layer:
            raise ValueError(f"Layer not found: {layer_name}")
    else:
        layer = datasource.GetLayer(0)

    if attribute_filter:
        if not validate_ogr_sql(attribute_filter):
            raise ValueError("Invalid SQL filter expression")
        layer.SetAttributeFilter(attribute_filter)

    if spatial_filter:
        if isinstance(spatial_filter, tuple) and len(spatial_filter) == 4:
            layer.SetSpatialFilterRect(*spatial_filter)
        elif isinstance(spatial_filter, str):
            filter_geom = ogr.CreateGeometryFromWkt(spatial_filter)
            layer.SetSpatialFilter(filter_geom)

    transform = None
    if target_crs:
        source_srs = layer.GetSpatialRef()
        if source_srs:
            transform = create_coordinate_transformation(source_srs, target_crs)

    layer_defn = layer.GetLayerDefn()

    chunk = []
    feature_id = 0

    for feature in layer:
        record = {'feature_id': feature_id}

        for i in range(feature.GetFieldCount()):
            field_defn = layer_defn.GetFieldDefn(i)
            field_name = field_defn.GetName()
            field_value = feature.GetField(i)

            record[field_name] = field_value

        geom = feature.GetGeometryRef()
        if geom:
            if transform:
                geom.Transform(transform)

            if geometry_format == 'wkt':
                record['geometry'] = geom.ExportToWkt()
            elif geometry_format == 'wkb':
                record['geometry'] = geom.ExportToWkb()
            elif geometry_format == 'geojson':
                record['geometry'] = geom.ExportToJson()
            elif geometry_format == 'geometry':
                record['geometry'] = {
                    'type': geom.GetGeometryName(),
                    'wkt': geom.ExportToWkt(),
                }

            record['geometry_type'] = geom.GetGeometryName()

        chunk.append(record)
        feature_id += 1

        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk

    datasource = None


def _read_vector_batch(
    file_paths: List[str],
    **kwargs
) -> Iterator[List[Dict[str, Any]]]:
    """
    Read multiple vector files in batch

    Args:
        file_paths: List of file paths to read
        **kwargs: Arguments passed to _read_vector

    Yields:
        Batches of features from all files
    """
    for file_path in file_paths:
        kwargs['file_path'] = file_path
        yield from _read_vector(**kwargs)


def _read_raster(
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
    Internal function to read raster data using GDAL

    Supports 200+ raster formats including:
    - GeoTIFF, COG, NetCDF, HDF5
    - ECW, MrSID, JPEG2000
    - DEM, SRTM elevation data
    """
    open_options = []
    for key, value in gdal_options.items():
        open_options.append(f"{key}={value}")

    dataset = gdal.OpenEx(file_path, gdal.OF_RASTER, open_options=open_options)
    if not dataset:
        raise ValueError(f"Cannot open raster file: {file_path}")

    if target_crs:
        resample_alg_map = {
            'nearest': gdal.GRA_NearestNeighbour,
            'bilinear': gdal.GRA_Bilinear,
            'cubic': gdal.GRA_Cubic,
            'average': gdal.GRA_Average,
        }

        warp_options = gdal.WarpOptions(
            dstSRS=target_crs,
            resampleAlg=resample_alg_map[resample_method],
            format='MEM'
        )
        dataset = gdal.Warp('', dataset, options=warp_options)

    geotransform = dataset.GetGeoTransform()
    projection = dataset.GetProjection()

    metadata = {
        'width': dataset.RasterXSize,
        'height': dataset.RasterYSize,
        'bands': dataset.RasterCount,
        'projection': projection,
        'geotransform': geotransform,
        'driver': dataset.GetDriver().ShortName,
    }

    bands_to_read = bands or list(range(1, dataset.RasterCount + 1))

    if window:
        xoff, yoff, xsize, ysize = window
    else:
        xoff, yoff = 0, 0
        xsize, ysize = dataset.RasterXSize, dataset.RasterYSize

    if chunk_size:
        chunk_width, chunk_height = chunk_size
    else:
        chunk_width, chunk_height = DEFAULT_RASTER_CHUNK_SIZE

    for band_idx in bands_to_read:
        band = dataset.GetRasterBand(band_idx)

        band_metadata = {
            'band': band_idx,
            'nodata': band.GetNoDataValue(),
            'data_type': gdal.GetDataTypeName(band.DataType),
            'color_interpretation': gdal.GetColorInterpretationName(
                band.GetColorInterpretation()
            ),
        }

        try:
            stats = band.GetStatistics(True, True)
            band_metadata['statistics'] = {
                'min': stats[0],
                'max': stats[1],
                'mean': stats[2],
                'std': stats[3],
            }
        except Exception:
            band_metadata['statistics'] = None

        for y in range(yoff, yoff + ysize, chunk_height):
            for x in range(xoff, xoff + xsize, chunk_width):
                win_width = min(chunk_width, xsize - (x - xoff))
                win_height = min(chunk_height, ysize - (y - yoff))

                data = band.ReadAsArray(x, y, win_width, win_height)

                if output_format == 'array':
                    output_data = data.tolist()
                elif output_format == 'bytes':
                    output_data = data.tobytes()
                elif output_format == 'base64':
                    output_data = base64.b64encode(data.tobytes()).decode('utf-8')
                else:
                    output_data = data.tobytes()

                yield {
                    'metadata': metadata,
                    'band_info': band_metadata,
                    'window': {
                        'xoff': x,
                        'yoff': y,
                        'width': win_width,
                        'height': win_height,
                    },
                    'data': output_data,
                    'data_shape': data.shape,
                }

    dataset = None
