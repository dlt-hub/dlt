"""
Apache Sedona transformers for distributed spatial operations

Created by: Baroudi Malek & Fawzi Hammami

These transformers leverage Sedona's distributed computing capabilities for
massive-scale spatial operations (millions to billions of features).
"""

from typing import Any, Callable, Dict, Iterator, List, Literal, Optional

try:
    from sedona.spark import SedonaContext
    from sedona.sql import st_functions as STF
    from sedona.sql import st_constructors as STC
    from sedona.sql import st_aggregates as STA
    from sedona.sql import st_predicates as STP
    HAS_SEDONA = True
except ImportError:
    HAS_SEDONA = False

from dlt.common.exceptions import MissingDependencyException
from dlt.sources.sedona.helpers import create_sedona_context
from dlt.sources.sedona.settings import (
    DEFAULT_SPARK_CONFIG,
    SEDONA_SPATIAL_PREDICATES,
)

if not HAS_SEDONA:
    raise MissingDependencyException(
        "Sedona transformers",
        ["apache-sedona", "pyspark"],
        "Install Apache Sedona: pip install 'dlt[sedona]'"
    )


def _sedona_spatial_join(
    items: Iterator[Dict[str, Any]],
    join_table_path: str,
    predicate: Literal[
        'intersects', 'contains', 'within', 'covers',
        'covered_by', 'touches', 'overlaps', 'crosses'
    ] = 'intersects',
    geometry_column: str = 'geometry',
    join_geometry_column: str = 'geometry',
    spark_config: Optional[Dict] = None,
    broadcast_join: bool = False,
) -> Iterator[Dict[str, Any]]:
    """
    Distributed spatial join using Apache Sedona
    
    Use this for joining millions of spatial features efficiently.
    
    Args:
        items: Input features from dlt pipeline
        join_table_path: Path to spatial table to join with
        predicate: Spatial predicate (intersects, within, etc.)
        geometry_column: Geometry column in input data
        join_geometry_column: Geometry column in join table
        spark_config: Spark configuration overrides
        broadcast_join: Use broadcast join for small join tables
        
    Yields:
        Features with joined attributes
        
    Example:
        ```python
        # Join 10M points with 1M polygons
        points | sedona_spatial_join(
            join_table_path='/data/zones.parquet',
            predicate='within',
            broadcast_join=True
        )
        ```
    """
    if predicate not in SEDONA_SPATIAL_PREDICATES:
        raise ValueError(f"Invalid predicate: {predicate}")
    
    sedona = create_sedona_context(spark_config=spark_config)
    
    # Convert items to Sedona DataFrame
    items_list = list(items)
    left_df = sedona.createDataFrame(items_list)
    
    # Load join table
    right_df = sedona.read.format('parquet').load(join_table_path)
    
    # Register temp views
    left_df.createOrReplaceTempView("left_table")
    right_df.createOrReplaceTempView("right_table")
    
    # Build spatial join query
    predicate_func = f"ST_{predicate.capitalize()}"
    
    if broadcast_join:
        hint = "/*+ BROADCAST(right_table) */"
    else:
        hint = ""
    
    query = f"""
        SELECT {hint} l.*, r.*
        FROM left_table l
        JOIN right_table r
        ON {predicate_func}(l.{geometry_column}, r.{join_geometry_column})
    """
    
    result_df = sedona.sql(query)
    
    # Convert back to dlt format
    for row in result_df.collect():
        yield row.asDict()
    
    sedona.sparkSession.stop()


def _sedona_buffer(
    items: Iterator[Dict[str, Any]],
    distance: float,
    geometry_column: str = 'geometry',
    spark_config: Optional[Dict] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Distributed buffer operation using Sedona
    
    Args:
        items: Input features
        distance: Buffer distance
        geometry_column: Geometry column name
        spark_config: Spark configuration
        
    Yields:
        Features with buffered geometries
    """
    sedona = create_sedona_context(spark_config=spark_config)
    
    items_list = list(items)
    df = sedona.createDataFrame(items_list)
    df.createOrReplaceTempView("input_data")
    
    result = sedona.sql(f"""
        SELECT 
            *,
            ST_Buffer({geometry_column}, {distance}) as {geometry_column}_buffered
        FROM input_data
    """)
    
    for row in result.collect():
        record = row.asDict()
        record[geometry_column] = record.pop(f'{geometry_column}_buffered')
        yield record
    
    sedona.sparkSession.stop()


def _sedona_aggregate(
    items: Iterator[Dict[str, Any]],
    group_by: List[str],
    aggregation: Literal['union', 'envelope', 'intersection'] = 'union',
    geometry_column: str = 'geometry',
    spark_config: Optional[Dict] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Distributed spatial aggregation using Sedona
    
    Aggregate geometries by groups (e.g., union all polygons by zone).
    
    Args:
        items: Input features
        group_by: Columns to group by
        aggregation: Spatial aggregation function
        geometry_column: Geometry column name
        spark_config: Spark configuration
        
    Yields:
        Aggregated features
        
    Example:
        ```python
        # Union all parcels by zone
        parcels | sedona_aggregate(
            group_by=['zone_id'],
            aggregation='union'
        )
        ```
    """
    sedona = create_sedona_context(spark_config=spark_config)
    
    items_list = list(items)
    df = sedona.createDataFrame(items_list)
    df.createOrReplaceTempView("input_data")
    
    group_cols = ', '.join(group_by)
    agg_func = f"ST_{aggregation.capitalize()}_Aggr"
    
    result = sedona.sql(f"""
        SELECT 
            {group_cols},
            {agg_func}({geometry_column}) as {geometry_column}
        FROM input_data
        GROUP BY {group_cols}
    """)
    
    for row in result.collect():
        yield row.asDict()
    
    sedona.sparkSession.stop()


def _sedona_cluster(
    items: Iterator[Dict[str, Any]],
    distance_threshold: float,
    min_points: int = 1,
    geometry_column: str = 'geometry',
    spark_config: Optional[Dict] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Spatial clustering using Sedona
    
    Cluster nearby features using DBSCAN algorithm.
    
    Args:
        items: Input features
        distance_threshold: Maximum distance for clustering
        min_points: Minimum points per cluster
        geometry_column: Geometry column
        spark_config: Spark configuration
        
    Yields:
        Features with cluster_id assigned
    """
    sedona = create_sedona_context(spark_config=spark_config)
    
    items_list = list(items)
    df = sedona.createDataFrame(items_list)
    
    # Add centroid for clustering
    df = df.withColumn('_centroid', STF.ST_Centroid(df[geometry_column]))
    
    # Perform spatial clustering (simplified DBSCAN)
    df.createOrReplaceTempView("input_data")
    
    result = sedona.sql(f"""
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY FLOOR(ST_X(_centroid) / {distance_threshold}),
                             FLOOR(ST_Y(_centroid) / {distance_threshold})
                ORDER BY ST_X(_centroid)
            ) as cluster_id
        FROM input_data
    """)
    
    for row in result.collect():
        record = row.asDict()
        record.pop('_centroid', None)
        yield record
    
    sedona.sparkSession.stop()


def _sedona_transform_crs(
    items: Iterator[Dict[str, Any]],
    source_crs: str = 'EPSG:4326',
    target_crs: str = 'EPSG:3857',
    geometry_column: str = 'geometry',
    spark_config: Optional[Dict] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Distributed CRS transformation using Sedona
    
    Args:
        items: Input features
        source_crs: Source CRS (e.g., 'EPSG:4326')
        target_crs: Target CRS (e.g., 'EPSG:3857')
        geometry_column: Geometry column
        spark_config: Spark configuration
        
    Yields:
        Features with transformed geometries
    """
    sedona = create_sedona_context(spark_config=spark_config)
    
    items_list = list(items)
    df = sedona.createDataFrame(items_list)
    df.createOrReplaceTempView("input_data")
    
    source_srid = int(source_crs.split(':')[1])
    target_srid = int(target_crs.split(':')[1])
    
    result = sedona.sql(f"""
        SELECT 
            *,
            ST_Transform(
                ST_SetSRID({geometry_column}, {source_srid}),
                {target_srid}
            ) as {geometry_column}_transformed
        FROM input_data
    """)
    
    for row in result.collect():
        record = row.asDict()
        record[geometry_column] = record.pop(f'{geometry_column}_transformed')
        yield record
    
    sedona.sparkSession.stop()


def _sedona_simplify(
    items: Iterator[Dict[str, Any]],
    tolerance: float,
    preserve_topology: bool = True,
    geometry_column: str = 'geometry',
    spark_config: Optional[Dict] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Distributed geometry simplification using Sedona
    
    Args:
        items: Input features
        tolerance: Simplification tolerance
        preserve_topology: Preserve topology during simplification
        geometry_column: Geometry column
        spark_config: Spark configuration
        
    Yields:
        Features with simplified geometries
    """
    sedona = create_sedona_context(spark_config=spark_config)
    
    items_list = list(items)
    df = sedona.createDataFrame(items_list)
    df.createOrReplaceTempView("input_data")
    
    simplify_func = 'ST_SimplifyPreserveTopology' if preserve_topology else 'ST_Simplify'
    
    result = sedona.sql(f"""
        SELECT 
            *,
            {simplify_func}({geometry_column}, {tolerance}) as {geometry_column}_simplified
        FROM input_data
    """)
    
    for row in result.collect():
        record = row.asDict()
        record[geometry_column] = record.pop(f'{geometry_column}_simplified')
        yield record
    
    sedona.sparkSession.stop()
