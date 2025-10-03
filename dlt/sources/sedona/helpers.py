"""
Helper functions and utilities for Apache Sedona integration

Created by: Baroudi Malek & Fawzi Hammami
"""

from typing import Any, Dict, Optional, Union
from contextlib import contextmanager

try:
    from pyspark.sql import SparkSession
    from sedona.spark import SedonaContext
    HAS_SEDONA = True
except ImportError:
    HAS_SEDONA = False

from dlt.common.exceptions import MissingDependencyException
from dlt.sources.sedona.settings import DEFAULT_SPARK_CONFIG, SEDONA_SPARK_CONFIG

if not HAS_SEDONA:
    raise MissingDependencyException(
        "Sedona sources",
        ["apache-sedona", "pyspark"],
        "Install Apache Sedona: pip install 'dlt[sedona]' or pip install apache-sedona pyspark"
    )


def create_sedona_context(
    spark_config: Optional[Dict[str, Any]] = None,
    app_name: str = "dlt-sedona",
    master: str = "local[*]",
) -> Any:
    """
    Create SedonaContext for distributed spatial processing
    
    Args:
        spark_config: Additional Spark configuration
        app_name: Spark application name
        master: Spark master URL
        
    Returns:
        SedonaContext instance
        
    Example:
        ```python
        sedona = create_sedona_context(
            spark_config={'spark.executor.memory': '4g'},
            master='spark://cluster:7077'
        )
        ```
    """
    config = {**DEFAULT_SPARK_CONFIG, **SEDONA_SPARK_CONFIG}
    config['spark.app.name'] = app_name
    config['spark.master'] = master
    
    if spark_config:
        config.update(spark_config)
    
    builder = SparkSession.builder
    for key, value in config.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    sedona = SedonaContext.create(spark)
    
    return sedona


@contextmanager
def SedonaContextManager(
    spark_config: Optional[Dict[str, Any]] = None,
    app_name: str = "dlt-sedona",
    master: str = "local[*]",
):
    """
    Context manager for SedonaContext
    
    Automatically creates and closes Sedona context.
    
    Example:
        ```python
        with SedonaContextManager() as sedona:
            df = sedona.sql("SELECT * FROM spatial_table")
            # Process data
        # Context automatically closed
        ```
    """
    sedona = create_sedona_context(spark_config, app_name, master)
    try:
        yield sedona
    finally:
        sedona.sparkSession.stop()


def register_spatial_table(
    sedona_context: Any,
    table_name: str,
    file_path: str,
    format: str = 'parquet',
    geometry_column: str = 'geometry',
) -> None:
    """
    Register spatial file as Sedona table
    
    Args:
        sedona_context: SedonaContext instance
        table_name: Name for the temporary table
        file_path: Path to spatial file
        format: File format (parquet, csv, shapefile, geojson)
        geometry_column: Name of geometry column
        
    Example:
        ```python
        register_spatial_table(
            sedona,
            'roads',
            '/data/roads.parquet',
            format='parquet'
        )
        
        result = sedona.sql("SELECT * FROM roads WHERE ST_Area(geometry) > 1000")
        ```
    """
    if format == 'parquet':
        df = sedona_context.read.format('geoparquet').load(file_path)
    elif format == 'csv':
        df = sedona_context.read.format('csv').option('header', 'true').load(file_path)
    elif format == 'shapefile':
        df = sedona_context.read.format('shapefile').load(file_path)
    elif format == 'geojson':
        df = sedona_context.read.format('geojson').load(file_path)
    else:
        df = sedona_context.read.format(format).load(file_path)
    
    df.createOrReplaceTempView(table_name)


def convert_to_sedona_geometry(
    sedona_context: Any,
    df: Any,
    geometry_column: str = 'geometry',
    geometry_format: str = 'wkt',
    source_crs: str = 'EPSG:4326',
) -> Any:
    """
    Convert geometry column to Sedona geometry type
    
    Args:
        sedona_context: SedonaContext instance
        df: Spark DataFrame
        geometry_column: Name of geometry column
        geometry_format: Input format (wkt, wkb, geojson)
        source_crs: Source coordinate reference system
        
    Returns:
        DataFrame with Sedona geometry column
    """
    from sedona.sql.st_constructors import ST_GeomFromWKT, ST_GeomFromWKB, ST_GeomFromGeoJSON
    from sedona.sql.st_functions import ST_SetSRID
    
    if geometry_format == 'wkt':
        df = df.withColumn(geometry_column, ST_GeomFromWKT(geometry_column))
    elif geometry_format == 'wkb':
        df = df.withColumn(geometry_column, ST_GeomFromWKB(geometry_column))
    elif geometry_format == 'geojson':
        df = df.withColumn(geometry_column, ST_GeomFromGeoJSON(geometry_column))
    
    # Set SRID
    epsg_code = int(source_crs.split(':')[1]) if ':' in source_crs else 4326
    df = df.withColumn(geometry_column, ST_SetSRID(geometry_column, epsg_code))
    
    return df


def optimize_sedona_dataframe(df: Any, partition_count: Optional[int] = None) -> Any:
    """
    Optimize Sedona DataFrame for spatial operations
    
    Args:
        df: Sedona DataFrame
        partition_count: Number of partitions (default: auto)
        
    Returns:
        Optimized DataFrame
    """
    if partition_count:
        df = df.repartition(partition_count)
    
    df = df.cache()
    
    return df


def get_sedona_version() -> str:
    """Get Apache Sedona version"""
    try:
        import sedona
        return sedona.__version__
    except (ImportError, AttributeError):
        return "unknown"


def validate_sedona_installation() -> bool:
    """
    Validate that Sedona is properly installed
    
    Returns:
        True if Sedona is properly configured
    """
    try:
        sedona = create_sedona_context(app_name="dlt-validation-test")
        
        # Test basic Sedona functionality
        test_df = sedona.sql("SELECT ST_Point(0.0, 0.0) as point")
        test_df.count()
        
        sedona.sparkSession.stop()
        return True
    except Exception as e:
        print(f"Sedona validation failed: {e}")
        return False
