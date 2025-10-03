"""
Apache Sedona readers for distributed spatial data

Created by: Baroudi Malek & Fawzi Hammami
"""

from typing import Any, Dict, Iterator, List, Optional

try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from sedona.sql.st_functions import ST_AsText, ST_AsGeoJSON, ST_AsBinary
    HAS_SEDONA = True
except ImportError:
    HAS_SEDONA = False

from dlt.common.exceptions import MissingDependencyException
from dlt.sources.sedona.settings import DEFAULT_BATCH_SIZE, GEOMETRY_OUTPUT_FORMATS

if not HAS_SEDONA:
    raise MissingDependencyException(
        "Sedona readers",
        ["apache-sedona", "pyspark"],
        "Install Apache Sedona: pip install 'dlt[sedona]'"
    )


def _read_sedona_table(
    sedona_dataframe: Any,
    batch_size: int = DEFAULT_BATCH_SIZE,
    geometry_format: str = 'wkt',
) -> Iterator[List[Dict[str, Any]]]:
    """
    Internal function to read Sedona DataFrame
    
    Converts Sedona spatial DataFrame to dlt-compatible batches.
    
    Args:
        sedona_dataframe: Sedona DataFrame with spatial data
        batch_size: Number of rows per batch
        geometry_format: Format for geometry output
    
    Yields:
        List[Dict[str, Any]]: Batches of records
    """
    if geometry_format not in GEOMETRY_OUTPUT_FORMATS:
        raise ValueError(
            f"Invalid geometry format: {geometry_format}. "
            f"Supported: {GEOMETRY_OUTPUT_FORMATS}"
        )
    
    # Identify geometry columns
    geometry_columns = []
    for field in sedona_dataframe.schema.fields:
        if 'geometry' in field.dataType.typeName().lower():
            geometry_columns.append(field.name)
    
    # Convert geometry columns to specified format
    df = sedona_dataframe
    for geom_col in geometry_columns:
        if geometry_format == 'wkt':
            df = df.withColumn(f"{geom_col}_temp", ST_AsText(geom_col))
        elif geometry_format == 'geojson':
            df = df.withColumn(f"{geom_col}_temp", ST_AsGeoJSON(geom_col))
        elif geometry_format == 'wkb':
            df = df.withColumn(f"{geom_col}_temp", ST_AsBinary(geom_col))
        
        df = df.drop(geom_col).withColumnRenamed(f"{geom_col}_temp", geom_col)
    
    # Convert to batches
    total_rows = df.count()
    num_batches = (total_rows + batch_size - 1) // batch_size
    
    for batch_idx in range(num_batches):
        offset = batch_idx * batch_size
        batch_df = df.limit(batch_size).offset(offset)
        
        # Convert to list of dicts
        batch = []
        for row in batch_df.collect():
            record = row.asDict()
            batch.append(record)
        
        if batch:
            yield batch


def _read_sedona_sql(
    sql: str,
    sedona_context: Any,
    batch_size: int = DEFAULT_BATCH_SIZE,
    geometry_format: str = 'wkt',
) -> Iterator[List[Dict[str, Any]]]:
    """
    Execute Sedona SQL and read results
    
    Args:
        sql: Sedona SQL query with spatial functions
        sedona_context: SedonaContext instance
        batch_size: Number of rows per batch
        geometry_format: Format for geometry output
        
    Yields:
        List[Dict[str, Any]]: Query results
    """
    df = sedona_context.sql(sql)
    
    yield from _read_sedona_table(df, batch_size, geometry_format)


def _read_spatial_rdd(
    spatial_rdd: Any,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Iterator[List[Dict[str, Any]]]:
    """
    Read Sedona SpatialRDD
    
    Args:
        spatial_rdd: Sedona SpatialRDD (PointRDD, PolygonRDD, etc.)
        batch_size: Number of features per batch
        
    Yields:
        List[Dict[str, Any]]: Spatial features
    """
    # Convert RDD to DataFrame
    from sedona.utils.adapter import Adapter
    
    df = Adapter.toDf(spatial_rdd, spatial_rdd.fieldNames)
    
    yield from _read_sedona_table(df, batch_size, 'wkt')


def _read_sedona_partitioned(
    sedona_dataframe: Any,
    partition_column: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    geometry_format: str = 'wkt',
) -> Iterator[List[Dict[str, Any]]]:
    """
    Read Sedona DataFrame with partition awareness
    
    More efficient for large datasets by processing partitions in parallel.
    
    Args:
        sedona_dataframe: Sedona DataFrame
        partition_column: Column to partition by
        batch_size: Rows per batch
        geometry_format: Geometry output format
        
    Yields:
        List[Dict[str, Any]]: Batches per partition
    """
    partitions = sedona_dataframe.select(partition_column).distinct().collect()
    
    for partition_row in partitions:
        partition_value = partition_row[partition_column]
        partition_df = sedona_dataframe.filter(
            sedona_dataframe[partition_column] == partition_value
        )
        
        yield from _read_sedona_table(partition_df, batch_size, geometry_format)


def _read_sedona_streaming(
    sedona_stream: Any,
    batch_duration: int = 60,
    geometry_format: str = 'wkt',
) -> Iterator[List[Dict[str, Any]]]:
    """
    Read from Sedona streaming query (Flink integration)
    
    For real-time spatial analytics.
    
    Args:
        sedona_stream: Sedona streaming DataFrame
        batch_duration: Duration in seconds for micro-batches
        geometry_format: Geometry output format
        
    Yields:
        List[Dict[str, Any]]: Stream batches
    """
    # Start streaming query
    query = sedona_stream.writeStream \
        .format("memory") \
        .queryName("dlt_stream") \
        .start()
    
    try:
        import time
        from pyspark.sql import SparkSession
        
        spark = SparkSession.getActiveSession()
        
        while query.isActive:
            time.sleep(batch_duration)
            
            # Read accumulated data
            batch_df = spark.sql("SELECT * FROM dlt_stream")
            
            if batch_df.count() > 0:
                yield from _read_sedona_table(batch_df, 10000, geometry_format)
                
                # Clear table for next batch
                spark.sql("TRUNCATE TABLE dlt_stream")
    finally:
        query.stop()
