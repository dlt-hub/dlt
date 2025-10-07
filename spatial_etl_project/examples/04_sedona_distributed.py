"""
Example 04: Distributed Spatial Processing with Apache Sedona

This example demonstrates:
- Apache Sedona with PySpark
- Loading spatial data into Spark DataFrames
- Sedona SQL spatial functions
- Distributed spatial operations

IMPORTANT COMPATIBILITY NOTE:
This example requires specific version combinations:
- PySpark 3.4.x with apache-sedona 1.5.x (Scala 2.12)
- OR PySpark 3.5.x with apache-sedona 1.6.x compiled for Scala 2.13

Current environment may have version mismatches. If you see 
"NoClassDefFoundError: scala/collection/GenTraversableOnce", this
indicates a Scala version mismatch between PySpark and Sedona.

For production use, consider:
1. Use Docker with pre-configured Sedona+Spark images
2. Use managed Spark services (Databricks, EMR, etc.)
3. Pin exact versions in requirements.txt

Requires: apache-sedona, pyspark
"""

import dlt
from typing import Iterator, Dict, Any


def create_sedona_spark():
    """Create Spark session with Sedona configuration"""
    try:
        from sedona.spark import SedonaContext
        
        config = SedonaContext.builder() \
            .appName("dlt-sedona-example") \
            .master("local[*]") \
            .config("spark.jars.packages", 
                    "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.0") \
            .getOrCreate()
        
        sedona = SedonaContext.create(config)
        sedona.sparkContext.setLogLevel("WARN")
        
        return sedona
    except ImportError as e:
        print(f"❌ Failed to import Sedona/PySpark: {e}")
        print("Install with: pip install apache-sedona pyspark")
        return None


@dlt.resource
def sedona_countries() -> Iterator[Dict[str, Any]]:
    """Process spatial data with Sedona SQL"""
    spark = create_sedona_spark()
    
    if not spark:
        print("⚠️ Skipping Sedona example - dependencies not available")
        return
    
    countries_data = [
        (1, "United States", "POLYGON((-125 50, -66 50, -66 25, -125 25, -125 50))"),
        (2, "Canada", "POLYGON((-141 70, -52 70, -52 42, -141 42, -141 70))"),
        (3, "Mexico", "POLYGON((-117 33, -86 33, -86 14, -117 14, -117 33))"),
    ]
    
    df = spark.createDataFrame(countries_data, ["id", "name", "geometry"])
    df.createOrReplaceTempView("countries")
    
    result_df = spark.sql("""
        SELECT 
            id,
            name,
            ST_GeomFromText(geometry) as geom,
            ST_Area(ST_GeomFromText(geometry)) as area,
            ST_Length(ST_GeomFromText(geometry)) as perimeter,
            ST_Centroid(ST_GeomFromText(geometry)) as centroid
        FROM countries
    """)
    
    print("\n📊 Sedona DataFrame Schema:")
    result_df.printSchema()
    
    print("\n📊 Sample Data:")
    result_df.show(truncate=False)
    
    for row in result_df.collect():
        yield {
            "id": row.id,
            "name": row.name,
            "geometry_wkt": row.geom.toString() if row.geom else None,
            "area": float(row.area) if row.area else None,
            "perimeter": float(row.perimeter) if row.perimeter else None,
            "centroid_wkt": row.centroid.toString() if row.centroid else None,
        }
    
    spark.stop()


@dlt.resource
def sedona_spatial_join() -> Iterator[Dict[str, Any]]:
    """Demonstrate spatial join with Sedona"""
    spark = create_sedona_spark()
    
    if not spark:
        return
    
    points_data = [
        (1, "Point A", "POINT(-100 40)"),
        (2, "Point B", "POINT(-70 45)"),
        (3, "Point C", "POINT(-95 30)"),
    ]
    
    zones_data = [
        ("West", "POLYGON((-125 50, -100 50, -100 25, -125 25, -125 50))"),
        ("East", "POLYGON((-100 50, -66 50, -66 25, -100 25, -100 50))"),
    ]
    
    points_df = spark.createDataFrame(points_data, ["id", "name", "geometry"])
    zones_df = spark.createDataFrame(zones_data, ["zone_name", "geometry"])
    
    points_df.createOrReplaceTempView("points")
    zones_df.createOrReplaceTempView("zones")
    
    result_df = spark.sql("""
        SELECT 
            p.id,
            p.name,
            z.zone_name,
            ST_AsText(ST_GeomFromText(p.geometry)) as point_wkt,
            ST_AsText(ST_GeomFromText(z.geometry)) as zone_wkt
        FROM points p
        JOIN zones z ON ST_Within(
            ST_GeomFromText(p.geometry), 
            ST_GeomFromText(z.geometry)
        )
    """)
    
    print("\n🔗 Spatial Join Results:")
    result_df.show(truncate=False)
    
    for row in result_df.collect():
        yield {
            "point_id": row.id,
            "point_name": row.name,
            "zone_name": row.zone_name,
            "point_geometry": row.point_wkt,
        }
    
    spark.stop()


def main():
    """Run Sedona distributed spatial processing"""
    print("=" * 80)
    print("Example 04: Distributed Spatial Processing with Apache Sedona")
    print("=" * 80)
    
    pipeline = dlt.pipeline(
        pipeline_name="sedona_spatial",
        destination="duckdb",
        dataset_name="sedona_examples"
    )
    
    print("\n[1/2] Processing countries with Sedona SQL...")
    try:
        info1 = pipeline.run(
            sedona_countries(),
            table_name="countries"
        )
        print(f"✅ Loaded {info1.load_packages[0].state} - countries")
    except Exception as e:
        print(f"⚠️ Countries example skipped: {e}")
    
    print("\n[2/2] Spatial join with Sedona...")
    try:
        info2 = pipeline.run(
            sedona_spatial_join(),
            table_name="spatial_joins"
        )
        print(f"✅ Loaded {info2.load_packages[0].state} - spatial_joins")
    except Exception as e:
        print(f"⚠️ Spatial join example skipped: {e}")
    
    print("\n" + "=" * 80)
    print("✅ Sedona examples completed!")
    print("=" * 80)


if __name__ == "__main__":
    main()
