import pytest
from pathlib import Path

import dlt


TEST_DATA_DIR = Path(__file__).parent / "data" / "spatial"


def test_sedona_spatial_pipeline_standalone():
    try:
        from sedona.spark import SedonaContext
        from pyspark.sql import SparkSession
        from dlt.sources.sedona import read_sedona_table
    except ImportError:
        pytest.skip("Sedona not installed")
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("dlt_sedona_test") \
        .config("spark.jars.packages", "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.0") \
        .config("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
        .getOrCreate()
    
    sedona = SedonaContext.create(spark)
    
    shp_file = str(TEST_DATA_DIR / "ne_110m_admin_0_countries.shp")
    
    df = sedona.read.format("shapefile").option("encoding", "UTF-8").load(shp_file)
    
    df_with_area = df.selectExpr(
        "*",
        "ST_Area(geometry) as area",
        "ST_Length(geometry) as perimeter"
    )
    
    print(f"Loaded {df_with_area.count()} countries from shapefile")
    print("Schema:")
    df_with_area.printSchema()
    print("\nFirst 5 rows:")
    df_with_area.show(5, truncate=False)
    
    pipeline = dlt.pipeline(
        pipeline_name="sedona_spatial_test",
        destination="duckdb",
        dataset_name="spatial_test"
    )
    
    info = pipeline.run(
        read_sedona_table(df_with_area, batch_size=50),
        table_name="world_countries"
    )
    
    print(f"\nPipeline run info: {info}")
    
    if hasattr(info, 'load_packages') and info.load_packages:
        print("Successfully loaded spatial data from Sedona DataFrame")
    
    spark.stop()
    
    assert len(info.load_packages) > 0
    print("\n✓ Test passed: Sedona spatial pipeline loaded data successfully")


def test_sedona_spatial_sql_standalone():
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
    
    shp_file = str(TEST_DATA_DIR / "ne_110m_admin_0_countries.shp")
    
    df = sedona.read.format("shapefile").option("encoding", "UTF-8").load(shp_file)
    df.createOrReplaceTempView("countries")
    
    spatial_query = """
        SELECT 
            NAME as country_name,
            CONTINENT,
            ST_AsText(geometry) as geometry_wkt,
            ST_Area(geometry) as area,
            ST_Centroid(geometry) as centroid,
            CASE 
                WHEN ST_Area(geometry) > 1000000 THEN 'Large'
                WHEN ST_Area(geometry) > 100000 THEN 'Medium'
                ELSE 'Small'
            END as size_category
        FROM countries
        WHERE ST_IsValid(geometry) = true
        ORDER BY area DESC
        LIMIT 50
    """
    
    print("Executing spatial SQL query...")
    result_df = sedona.sql(spatial_query)
    print(f"Query returned {result_df.count()} rows")
    print("\nTop 5 largest countries:")
    result_df.show(5, truncate=False)
    
    pipeline = dlt.pipeline(
        pipeline_name="sedona_sql_test",
        destination="duckdb",
        dataset_name="spatial_sql_test"
    )
    
    info = pipeline.run(
        read_sedona_sql(spatial_query, sedona, batch_size=25),
        table_name="country_analysis"
    )
    
    print(f"\nPipeline run info: {info}")
    
    if hasattr(info, 'load_packages') and info.load_packages:
        print("Successfully loaded spatial data from Sedona SQL query")
    
    spark.stop()
    
    assert len(info.load_packages) > 0
    print("\n✓ Test passed: Sedona SQL spatial query pipeline loaded data successfully")


if __name__ == "__main__":
    print("=" * 80)
    print("Testing Sedona Spatial ETL Integration")
    print("=" * 80)
    
    try:
        print("\n[1/2] Running Sedona DataFrame test...")
        test_sedona_spatial_pipeline_standalone()
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    
    try:
        print("\n[2/2] Running Sedona SQL test...")
        test_sedona_spatial_sql_standalone()
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("All tests completed!")
    print("=" * 80)
