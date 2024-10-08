from typing import List

from shapely import (  # type: ignore
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
    LinearRing,
)
from shapely.wkb import dumps as wkb_dumps  # type: ignore

from dlt.common.typing import DictStrStr


def generate_sample_geometry_records(srid: int = 4326) -> List[DictStrStr]:
    """
    Generate sample geometry records including WKT and WKB representations.

    Args:
        srid (int): Default SRID to use in generation procedure.

    Returns:
        A list of dictionaries, each containing a geometry type,
        its Well-Known Text (WKT), and Well-Known Binary (WKB) representation.
    """
    point = Point(1, 1)
    line_string = LineString([(0, 0), (1, 1), (2, 2)])
    polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    multi_point = MultiPoint([(0, 0), (1, 1), (2, 2)])
    multi_line = MultiLineString([((0, 0), (1, 1)), ((2, 2), (3, 3))])
    multi_polygon = MultiPolygon([polygon, Polygon([(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)])])
    geometry_collection = GeometryCollection([point, line_string])
    linear_ring = LinearRing([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    complex_polygon = Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)], [[(4, 4), (6, 4), (6, 6), (4, 6), (4, 4)]]
    )

    empty_point = Point()
    empty_line_string = LineString()
    empty_polygon = Polygon()
    empty_multi_point = MultiPoint()
    empty_multi_line = MultiLineString()
    empty_multi_polygon = MultiPolygon()
    empty_geometry_collection = GeometryCollection()

    geometries = [
        ("Point", point),
        ("LineString", line_string),
        ("Polygon", polygon),
        ("MultiPoint", multi_point),
        ("MultiLineString", multi_line),
        ("MultiPolygon", multi_polygon),
        ("GeometryCollection", geometry_collection),
        ("LinearRing", linear_ring),
        ("ComplexPolygon", complex_polygon),
        ("EmptyPoint", empty_point),
        ("EmptyLineString", empty_line_string),
        ("EmptyPolygon", empty_polygon),
        ("EmptyMultiPoint", empty_multi_point),
        ("EmptyMultiLineString", empty_multi_line),
        ("EmptyMultiPolygon", empty_multi_polygon),
        ("EmptyGeometryCollection", empty_geometry_collection),
    ]
    return [
        {
            "type": name,
            "wkt": geom.wkt,
            "wkb_binary": wkb_dumps(geom, srid=srid),
            "wkb_hex_str": wkb_dumps(geom, srid=srid).hex(),
        }
        for name, geom in geometries
    ]
