"""
Simplified Sedona test using WKT strings instead of shapefile reading
This avoids the shapefile data source dependency
"""
import pytest
from pathlib import Path

import dlt


TEST_DATA_DIR = Path(__file__).parent / "data" / "spatial"


def test_sedona_wkt_pipeline():
    """
    Test Sedona with WKT geometries (no shapefile reader needed)
    """
    try:
        from sedona.spark import SedonaContext
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        from dlt.sources.sedona import read_sedona_table
    except ImportError:
        pytest.skip("Sedona not installed")
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("dlt_sedona_wkt_test") \
        .config("spark.jars.packages", "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.0") \
        .config("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
        .getOrCreate()
    
    sedona = SedonaContext.create(spark)
    
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("geom_wkt", StringType(), False),
    ])
    
    data = [
        (1, "United States", "POLYGON((-125 50, -66 50, -66 25, -125 25, -125 50))"),
        (2, "Canada", "POLYGON((-141 70, -52 70, -52 42, -141 42, -141 70))"),
        (3, "Mexico", "POLYGON((-117 33, -86 33, -86 14, -117 14, -117 33))"),
        (4, "Brazil", "POLYGON((-74 5, -34 5, -34 -34, -74 -34, -74 5))"),
        (5, "Argentina", "POLYGON((-74 -22, -53 -22, -53 -55, -74 -55, -74 -22))"),
    ]
    
    df = sedona.createDataFrame(data, schema)
    
    df_with_geom = df.selectExpr(
        "id",
        "name",
        "ST_GeomFromText(geom_wkt) as geometry",
        "ST_Area(ST_GeomFromText(geom_wkt)) as area",
        "ST_Length(ST_GeomFromText(geom_wkt)) as perimeter",
        "ST_Centroid(ST_GeomFromText(geom_wkt)) as centroid"
    )
    
    print(f"Created DataFrame with {df_with_geom.count()} countries")
    print("\nSchema:")
    df_with_geom.printSchema()
    print("\nData:")
    df_with_geom.show(truncate=False)
    
    pipeline = dlt.pipeline(
        pipeline_name="sedona_wkt_test",
        destination="duckdb",
        dataset_name="spatial_wkt"
    )
    
    info = pipeline.run(
        read_sedona_table(df_with_geom, batch_size=10, geometry_format='wkt'),
        table_name="countries"
    )
    
    print(f"\nPipeline run info: {info}")
    
    if hasattr(info, 'load_packages') and info.load_packages:
        print("Successfully loaded Sedona spatial data with WKT geometries")
    
    spark.stop()
    
    assert len(info.load_packages) > 0
    assert info.load_packages[0].state == "loaded"
    print("\n✓ Test passed: Sedona WKT pipeline loaded data successfully")


def test_sedona_sql_query():
    """
    Test Sedona SQL with spatial operations
    """
    try:
        from sedona.spark import SedonaContext
        from pyspark.sql import SparkSession
        from dlt.sources.sedona import read_sedona_sql
    except ImportError:
        pytest.skip("Sedona not installed")
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("dlt_sedona_sql_test") \
        .config("spark.jars.packages", "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.0") \
        .config("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
        .getOrCreate()
    
    sedona = SedonaContext.create(spark)
    
    sedona.sql("""
        CREATE OR REPLACE TEMP VIEW test_points AS
        SELECT 
            1 as id, 
            'Point A' as name,
            ST_Point(-73.935242, 40.730610) as geometry
        UNION ALL
        SELECT 
            2 as id,
            'Point B' as name, 
            ST_Point(-118.243683, 34.052235) as geometry
        UNION ALL
        SELECT
            3 as id,
            'Point C' as name,
            ST_Point(-87.629798, 41.878114) as geometry
    """)
    
    spatial_query = """
        SELECT 
            id,
            name,
            ST_AsText(geometry) as geometry_wkt,
            ST_X(geometry) as longitude,
            ST_Y(geometry) as latitude,
            ST_Distance(geometry, ST_Point(0, 0)) as dist_from_origin,
            CASE 
                WHEN ST_X(geometry) < -100 THEN 'West'
                WHEN ST_X(geometry) < -80 THEN 'Central'
                ELSE 'East'
            END as region
        FROM test_points
        ORDER BY id
    """
    
    print("Executing Sedona SQL query...")
    result_df = sedona.sql(spatial_query)
    print(f"Query returned {result_df.count()} rows")
    print("\nResults:")
    result_df.show(truncate=False)
    
    pipeline = dlt.pipeline(
        pipeline_name="sedona_sql_test",
        destination="duckdb",
        dataset_name="spatial_sql"
    )
    
    info = pipeline.run(
        read_sedona_sql(spatial_query, sedona, batch_size=10),
        table_name="points_analyzed"
    )
    
    print(f"\nPipeline run info: {info}")
    
    if hasattr(info, 'load_packages') and info.load_packages:
        print("Successfully loaded Sedona SQL query results")
    
    spark.stop()
    
    assert len(info.load_packages) > 0
    assert info.load_packages[0].state == "loaded"
    print("\n✓ Test passed: Sedona SQL query pipeline loaded data successfully")


if __name__ == "__main__":
    print("=" * 80)
    print("Testing Sedona Spatial ETL Integration (Simplified)")
    print("=" * 80)
    
    try:
        print("\n[1/2] Running Sedona WKT test...")
        test_sedona_wkt_pipeline()
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    
    try:
        print("\n[2/2] Running Sedona SQL test...")
        test_sedona_sql_query()
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("All tests completed!")
    print("=" * 80)
