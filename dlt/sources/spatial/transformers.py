"""Spatial transformation functions - comparable to FME Transformers"""

from typing import Any, Callable, Dict, Iterator, List, Literal, Optional, Tuple, Union

try:
    from osgeo import ogr, osr
    from shapely import wkt, wkb
    from shapely.geometry import shape, mapping
    from shapely.ops import transform as shapely_transform
    import pyproj
    ogr.UseExceptions()
    osr.UseExceptions()
    HAS_SPATIAL_LIBS = True
except ImportError:
    HAS_SPATIAL_LIBS = False

from dlt.common.exceptions import MissingDependencyException

if not HAS_SPATIAL_LIBS:
    raise MissingDependencyException(
        "Spatial transformers",
        ["gdal", "shapely", "pyproj"],
        "Install spatial libraries: pip install 'dlt[spatial]'"
    )


def _reproject(
    items: Iterator[Dict[str, Any]],
    source_crs: str = 'EPSG:4326',
    target_crs: str = 'EPSG:3857',
    geometry_field: str = 'geometry',
) -> Iterator[Dict[str, Any]]:
    """
    Reproject geometries between coordinate reference systems

    Similar to FME Reprojector transformer

    Args:
        items: Iterator of feature dictionaries
        source_crs: Source CRS (e.g., 'EPSG:4326')
        target_crs: Target CRS (e.g., 'EPSG:3857')
        geometry_field: Name of geometry field

    Yields:
        Features with reprojected geometries

    Example:
        ```python
        roads | reproject(source_crs='EPSG:2154', target_crs='EPSG:4326')
        ```
    """
    project = pyproj.Transformer.from_crs(
        source_crs, target_crs, always_xy=True
    ).transform

    for item in items:
        if geometry_field in item and item[geometry_field]:
            try:
                geom = wkt.loads(item[geometry_field])
                reprojected = shapely_transform(project, geom)
                item[geometry_field] = reprojected.wkt
                item['_reprojected'] = True
                item['_source_crs'] = source_crs
                item['_target_crs'] = target_crs
            except Exception as e:
                item['_reproject_error'] = str(e)

        yield item


def _buffer_geometry(
    items: Iterator[Dict[str, Any]],
    distance: float,
    geometry_field: str = 'geometry',
    cap_style: Literal['round', 'flat', 'square'] = 'round',
    join_style: Literal['round', 'mitre', 'bevel'] = 'round',
    resolution: int = 16,
) -> Iterator[Dict[str, Any]]:
    """
    Apply buffer operation to geometries

    Similar to FME Bufferer transformer

    Args:
        items: Iterator of feature dictionaries
        distance: Buffer distance (in units of the geometry CRS)
        geometry_field: Name of geometry field
        cap_style: Style for line caps
        join_style: Style for line joins
        resolution: Number of segments per quarter circle

    Yields:
        Features with buffered geometries

    Example:
        ```python
        roads | buffer_geometry(distance=10)
        ```
    """
    cap_style_map = {'round': 1, 'flat': 2, 'square': 3}
    join_style_map = {'round': 1, 'mitre': 2, 'bevel': 3}

    for item in items:
        if geometry_field in item and item[geometry_field]:
            try:
                geom = wkt.loads(item[geometry_field])
                buffered = geom.buffer(
                    distance,
                    cap_style=cap_style_map[cap_style],
                    join_style=join_style_map[join_style],
                    resolution=resolution
                )
                item[geometry_field] = buffered.wkt
                item['geometry_type'] = buffered.geom_type
                item['_buffered'] = True
                item['_buffer_distance'] = distance
            except Exception as e:
                item['_buffer_error'] = str(e)

        yield item


def _spatial_filter(
    items: Iterator[Dict[str, Any]],
    filter_geometry: str,
    predicate: Literal['intersects', 'contains', 'within', 'touches', 'crosses', 'overlaps'] = 'intersects',
    geometry_field: str = 'geometry',
) -> Iterator[Dict[str, Any]]:
    """
    Filter features based on spatial relationship

    Similar to FME SpatialFilter transformer

    Args:
        items: Iterator of feature dictionaries
        filter_geometry: WKT geometry to filter against
        predicate: Spatial predicate to use
        geometry_field: Name of geometry field

    Yields:
        Features that match the spatial predicate

    Example:
        ```python
        buildings | spatial_filter(
            filter_geometry='POLYGON((...))',
            predicate='within'
        )
        ```
    """
    filter_geom = wkt.loads(filter_geometry)

    for item in items:
        if geometry_field in item and item[geometry_field]:
            try:
                geom = wkt.loads(item[geometry_field])
                predicate_func = getattr(geom, predicate)

                if predicate_func(filter_geom):
                    item['_spatial_match'] = True
                    yield item
            except Exception as e:
                item['_spatial_filter_error'] = str(e)
                yield item


def _validate_geometry(
    items: Iterator[Dict[str, Any]],
    geometry_field: str = 'geometry',
    repair: bool = True,
    remove_invalid: bool = False,
) -> Iterator[Dict[str, Any]]:
    """
    Validate and optionally repair invalid geometries

    Similar to FME GeometryValidator transformer

    Args:
        items: Iterator of feature dictionaries
        geometry_field: Name of geometry field
        repair: Attempt to repair invalid geometries (buffer(0) method)
        remove_invalid: Remove features with invalid geometries

    Yields:
        Features with validated/repaired geometries

    Example:
        ```python
        polygons | validate_geometry(repair=True)
        ```
    """
    for item in items:
        if geometry_field in item and item[geometry_field]:
            try:
                geom = wkt.loads(item[geometry_field])

                if not geom.is_valid:
                    item['_geometry_valid'] = False
                    item['_validation_error'] = 'Invalid geometry'

                    if repair:
                        geom = geom.buffer(0)
                        if geom.is_valid:
                            item[geometry_field] = geom.wkt
                            item['_geometry_repaired'] = True
                            item['_geometry_valid'] = True

                    if remove_invalid and not geom.is_valid:
                        continue
                else:
                    item['_geometry_valid'] = True

            except Exception as e:
                item['_validation_error'] = str(e)
                if remove_invalid:
                    continue

        yield item


def _simplify_geometry(
    items: Iterator[Dict[str, Any]],
    tolerance: float,
    geometry_field: str = 'geometry',
    preserve_topology: bool = True,
) -> Iterator[Dict[str, Any]]:
    """
    Simplify geometries using Douglas-Peucker algorithm

    Similar to FME Generalizer transformer

    Args:
        items: Iterator of feature dictionaries
        tolerance: Simplification tolerance
        geometry_field: Name of geometry field
        preserve_topology: Whether to preserve topology

    Yields:
        Features with simplified geometries

    Example:
        ```python
        coastlines | simplify_geometry(tolerance=10.0)
        ```
    """
    for item in items:
        if geometry_field in item and item[geometry_field]:
            try:
                geom = wkt.loads(item[geometry_field])

                if preserve_topology:
                    simplified = geom.simplify(tolerance, preserve_topology=True)
                else:
                    simplified = geom.simplify(tolerance)

                item[geometry_field] = simplified.wkt
                item['_simplified'] = True
                item['_simplification_tolerance'] = tolerance

            except Exception as e:
                item['_simplify_error'] = str(e)

        yield item


def _spatial_join(
    items: Iterator[Dict[str, Any]],
    join_features: List[Dict[str, Any]],
    predicate: Literal['intersects', 'contains', 'within'] = 'intersects',
    geometry_field: str = 'geometry',
    join_geometry_field: str = 'geometry',
    join_attributes: Optional[List[str]] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Perform spatial join between features

    Similar to FME PointOnAreaOverlayer or SpatialFilter with attribute transfer

    Args:
        items: Iterator of feature dictionaries (left dataset)
        join_features: List of features to join with (right dataset)
        predicate: Spatial predicate for join
        geometry_field: Geometry field in items
        join_geometry_field: Geometry field in join_features
        join_attributes: List of attribute names to transfer from join features

    Yields:
        Features with joined attributes

    Example:
        ```python
        points | spatial_join(
            join_features=polygons_list,
            predicate='within',
            join_attributes=['zone_name', 'zone_id']
        )
        ```
    """
    join_geoms = []
    for join_feat in join_features:
        if join_geometry_field in join_feat and join_feat[join_geometry_field]:
            geom = wkt.loads(join_feat[join_geometry_field])
            join_geoms.append((geom, join_feat))

    for item in items:
        if geometry_field in item and item[geometry_field]:
            try:
                geom = wkt.loads(item[geometry_field])
                matched = False

                for join_geom, join_feat in join_geoms:
                    predicate_func = getattr(geom, predicate)

                    if predicate_func(join_geom):
                        matched = True

                        if join_attributes:
                            for attr in join_attributes:
                                if attr in join_feat:
                                    item[f'joined_{attr}'] = join_feat[attr]
                        else:
                            for key, value in join_feat.items():
                                if key != join_geometry_field:
                                    item[f'joined_{key}'] = value
                        break

                item['_spatial_join_matched'] = matched

            except Exception as e:
                item['_spatial_join_error'] = str(e)

        yield item


def _attribute_mapper(
    items: Iterator[Dict[str, Any]],
    mapping: Dict[str, str],
    type_conversions: Optional[Dict[str, Callable]] = None,
    drop_unmapped: bool = False,
) -> Iterator[Dict[str, Any]]:
    """
    Map and convert attributes

    Similar to FME AttributeRenamer and AttributeManager

    Args:
        items: Iterator of feature dictionaries
        mapping: Dictionary mapping old names to new names
        type_conversions: Dictionary of attribute name to conversion function
        drop_unmapped: Drop attributes not in mapping

    Yields:
        Features with mapped attributes

    Example:
        ```python
        features | attribute_mapper(
            mapping={'OLD_NAME': 'new_name', 'HEIGHT': 'building_height'},
            type_conversions={'building_height': float}
        )
        ```
    """
    for item in items:
        new_item = {}

        for old_key, new_key in mapping.items():
            if old_key in item:
                value = item[old_key]

                if type_conversions and new_key in type_conversions:
                    try:
                        value = type_conversions[new_key](value)
                    except Exception as e:
                        new_item[f'_{new_key}_conversion_error'] = str(e)

                new_item[new_key] = value

        if not drop_unmapped:
            for key, value in item.items():
                if key not in mapping and key not in new_item:
                    new_item[key] = value

        yield new_item


def _geometry_extractor(
    items: Iterator[Dict[str, Any]],
    geometry_field: str = 'geometry',
    extract_coords: bool = True,
    extract_bounds: bool = True,
    extract_centroid: bool = False,
    extract_area: bool = False,
    extract_length: bool = False,
) -> Iterator[Dict[str, Any]]:
    """
    Extract geometric properties from geometries

    Similar to FME GeometryPropertyExtractor

    Args:
        items: Iterator of feature dictionaries
        geometry_field: Name of geometry field
        extract_coords: Extract coordinate list
        extract_bounds: Extract bounding box
        extract_centroid: Extract centroid coordinates
        extract_area: Extract area (for polygons)
        extract_length: Extract length (for lines)

    Yields:
        Features with extracted geometric properties

    Example:
        ```python
        polygons | geometry_extractor(
            extract_area=True,
            extract_centroid=True
        )
        ```
    """
    for item in items:
        if geometry_field in item and item[geometry_field]:
            try:
                geom = wkt.loads(item[geometry_field])

                if extract_coords:
                    item['_coordinates'] = list(geom.coords) if hasattr(geom, 'coords') else None

                if extract_bounds:
                    bounds = geom.bounds
                    item['_bounds'] = {
                        'minx': bounds[0],
                        'miny': bounds[1],
                        'maxx': bounds[2],
                        'maxy': bounds[3],
                    }

                if extract_centroid:
                    centroid = geom.centroid
                    item['_centroid_x'] = centroid.x
                    item['_centroid_y'] = centroid.y

                if extract_area and geom.geom_type in ['Polygon', 'MultiPolygon']:
                    item['_area'] = geom.area

                if extract_length and geom.geom_type in ['LineString', 'MultiLineString']:
                    item['_length'] = geom.length

            except Exception as e:
                item['_geometry_extract_error'] = str(e)

        yield item
