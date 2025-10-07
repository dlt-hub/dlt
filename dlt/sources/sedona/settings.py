"""
Configuration settings for Apache Sedona integration

Created by: Baroudi Malek & Fawzi Hammami
"""

import os
from typing import Dict, Any

GDAL_LIBRARY_PATH = os.getenv('GDAL_LIBRARY_PATH', '/opt/homebrew/lib/libgdal.dylib')
GEOS_LIBRARY_PATH = os.getenv('GEOS_LIBRARY_PATH', '/opt/homebrew/lib/libgeos_c.dylib')

DEFAULT_BATCH_SIZE = 10000

DEFAULT_SPARK_CONFIG: Dict[str, Any] = {
    'spark.app.name': 'dlt-sedona-pipeline',
    'spark.master': 'local[*]',
    'spark.sql.adaptive.enabled': 'true',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.kryo.registrator': 'org.apache.sedona.core.serde.SedonaKryoRegistrator',
}

SEDONA_SPARK_CONFIG: Dict[str, Any] = {
    'spark.sql.extensions': 'org.apache.sedona.sql.SedonaSqlExtensions',
    'spark.jars.packages': (
        'org.apache.sedona:sedona-spark-3.4_2.12:1.5.1,'
        'org.datasyslab:geotools-wrapper:1.5.1-28.2'
    ),
}

GEOMETRY_OUTPUT_FORMATS = ['wkt', 'wkb', 'geojson', 'ewkt']

SEDONA_SPATIAL_FUNCTIONS = [
    'ST_Area',
    'ST_Buffer',
    'ST_Centroid',
    'ST_Contains',
    'ST_ConvexHull',
    'ST_Distance',
    'ST_Envelope',
    'ST_Intersection',
    'ST_Intersects',
    'ST_Length',
    'ST_Simplify',
    'ST_Transform',
    'ST_Union',
    'ST_Within',
]

SEDONA_AGGREGATION_FUNCTIONS = [
    'ST_Envelope_Aggr',
    'ST_Union_Aggr',
    'ST_Intersection_Aggr',
]

SEDONA_JOIN_TYPES = ['inner', 'left', 'right', 'outer']

SEDONA_SPATIAL_PREDICATES = [
    'intersects',
    'contains',
    'within',
    'covers',
    'covered_by',
    'touches',
    'overlaps',
    'crosses',
    'equals',
]
