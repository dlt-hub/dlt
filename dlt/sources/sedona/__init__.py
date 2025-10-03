"""
Apache Sedona source for dlt - distributed spatial data processing

Created by: Baroudi Malek & Fawzi Hammami

Apache Sedona is a distributed geospatial computing engine for processing massive-scale
spatial datasets. This module provides seamless integration between dlt and Sedona,
enabling:

- Distributed spatial data reading (Spark/Flink)
- Massive-scale spatial transformations (300+ spatial functions)
- Real-time spatial analytics
- Integration with existing dlt spatial module

Use Sedona when:
- Processing millions to billions of spatial features
- Performing complex spatial joins across large datasets
- Need distributed computing for spatial operations
- Require real-time spatial analytics with Flink

Example:
    ```python
    import dlt
    from dlt.sources.sedona import read_sedona_table, sedona_spatial_join

    # Create Sedona context
    from sedona.spark import SedonaContext
    sedona = SedonaContext.create(spark)
    
    # Process massive spatial dataset with Sedona
    result = sedona.sql('''
        SELECT a.*, b.zone_name
        FROM roads a
        JOIN zones b
        WHERE ST_Within(a.geometry, b.geometry)
    ''')
    
    # Load Sedona results to data warehouse with dlt
    pipeline = dlt.pipeline(
        destination='snowflake',
        dataset_name='spatial_analytics'
    )
    
    pipeline.run(
        read_sedona_table(result),
        table_name='roads_by_zone'
    )
    ```

Integration with dlt spatial module:
    ```python
    from dlt.sources.spatial import read_vector
    from dlt.sources.sedona import sedona_process
    
    # Read with dlt, process with Sedona for scale
    roads = read_vector('/data/roads.shp')
    
    # Use Sedona for distributed processing when dataset is large
    roads_processed = roads | sedona_process(
        operation='buffer',
        distance=100,
        spark_config={'spark.master': 'spark://cluster:7077'}
    )
    ```
"""

from typing import Any, Dict, Iterator, List, Optional, Union

import dlt
from dlt.extract import decorators
from dlt.sources import DltResource

from dlt.sources.sedona.readers import (
    _read_sedona_table,
    _read_sedona_sql,
    _read_spatial_rdd,
)
from dlt.sources.sedona.transformers import (
    _sedona_spatial_join,
    _sedona_buffer,
    _sedona_aggregate,
    _sedona_cluster,
    _sedona_transform_crs,
)
from dlt.sources.sedona.helpers import (
    create_sedona_context,
    SedonaContextManager,
)
from dlt.sources.sedona.settings import (
    DEFAULT_SPARK_CONFIG,
    DEFAULT_BATCH_SIZE,
)


@decorators.resource
def read_sedona_table(
    sedona_dataframe: Any,
    batch_size: int = DEFAULT_BATCH_SIZE,
    geometry_format: str = 'wkt',
) -> Iterator[List[Dict[str, Any]]]:
    """
    Read data from Sedona DataFrame into dlt pipeline
    
    Converts Sedona spatial DataFrame to dlt-compatible format for loading
    to data warehouses.
    
    Args:
        sedona_dataframe: Sedona DataFrame with spatial data
        batch_size: Number of rows per batch (default: 10000)
        geometry_format: Format for geometry column ('wkt', 'wkb', 'geojson')
        
    Yields:
        List[Dict[str, Any]]: Batches of records
        
    Example:
        ```python
        from sedona.spark import SedonaContext
        import dlt
        
        sedona = SedonaContext.create(spark)
        df = sedona.sql("SELECT * FROM spatial_table WHERE ST_Area(geometry) > 1000")
        
        pipeline = dlt.pipeline(destination='postgres')
        pipeline.run(read_sedona_table(df), table_name='large_polygons')
        ```
    """
    yield from _read_sedona_table(sedona_dataframe, batch_size, geometry_format)


@decorators.resource
def read_sedona_sql(
    sql: str,
    sedona_context: Any,
    batch_size: int = DEFAULT_BATCH_SIZE,
    geometry_format: str = 'wkt',
) -> Iterator[List[Dict[str, Any]]]:
    """
    Execute Sedona SQL query and read results into dlt pipeline
    
    Enables direct SQL querying with Sedona's spatial functions.
    
    Args:
        sql: Sedona SQL query with spatial functions
        sedona_context: SedonaContext instance
        batch_size: Number of rows per batch
        geometry_format: Format for geometry column
        
    Yields:
        List[Dict[str, Any]]: Query results
        
    Example:
        ```python
        query = '''
            SELECT 
                a.id,
                a.name,
                b.zone_name,
                ST_Area(a.geometry) as area
            FROM buildings a
            JOIN zones b
            WHERE ST_Within(a.geometry, b.geometry)
        '''
        
        pipeline.run(
            read_sedona_sql(query, sedona_context),
            table_name='buildings_by_zone'
        )
        ```
    """
    yield from _read_sedona_sql(sql, sedona_context, batch_size, geometry_format)


@decorators.resource
def read_spatial_rdd(
    spatial_rdd: Any,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Iterator[List[Dict[str, Any]]]:
    """
    Read Sedona SpatialRDD into dlt pipeline
    
    For advanced users working directly with Sedona's RDD API.
    
    Args:
        spatial_rdd: Sedona SpatialRDD (PointRDD, PolygonRDD, etc.)
        batch_size: Number of features per batch
        
    Yields:
        List[Dict[str, Any]]: Spatial features
    """
    yield from _read_spatial_rdd(spatial_rdd, batch_size)


# Transformer decorators
sedona_spatial_join = decorators.transformer()(_sedona_spatial_join)
sedona_buffer = decorators.transformer()(_sedona_buffer)
sedona_aggregate = decorators.transformer()(_sedona_aggregate)
sedona_cluster = decorators.transformer()(_sedona_cluster)
sedona_transform_crs = decorators.transformer()(_sedona_transform_crs)


__all__ = [
    'read_sedona_table',
    'read_sedona_sql',
    'read_spatial_rdd',
    'sedona_spatial_join',
    'sedona_buffer',
    'sedona_aggregate',
    'sedona_cluster',
    'sedona_transform_crs',
    'create_sedona_context',
    'SedonaContextManager',
    'DEFAULT_SPARK_CONFIG',
    'DEFAULT_BATCH_SIZE',
]
