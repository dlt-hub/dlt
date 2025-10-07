"""
Example 04: Distributed Spatial Processing with Apache Sedona (Reference Implementation)

⚠️  IMPORTANT: This example demonstrates Sedona cluster architecture but has
    a known dependency issue (cdm-core unavailable in Maven Central).

    For WORKING spatial processing, use:
    - examples/01_simple_geojson.py
    - examples/03_spatial_transforms.py
    
    These provide all spatial operations without Sedona dependency issues.

For production Sedona deployments, use managed services:
- Databricks (Sedona pre-configured)
- AWS EMR (add Sedona bootstrap action)
- Google Dataproc (Sedona initialization script)

This file serves as a reference for Sedona SQL syntax and cluster patterns.
"""

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext
import os


def create_sedona_cluster_context():
    """Create Sedona context for cluster mode with proper repository configuration"""
    master_url = os.getenv("SPARK_MASTER_URL", "local[*]")
    
    print(f"Connecting to Spark Master: {master_url}")
    
    builder = SparkSession.builder \
        .appName("Sedona-Spatial-ETL-Cluster") \
        .master(master_url)
    
    config = builder \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
        .getOrCreate()
    
    # Create Sedona context
    sedona = SedonaContext.create(config)
    sedona.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Sedona Context created successfully")
    print(f"   Spark Version: {sedona.version}")
    print(f"   Master URL: {sedona.sparkContext.master}")
    print()
    
    return sedona


def example_1_country_processing(sedona):
    """Process countries with Sedona SQL"""
    print("\n" + "=" * 80)
    print("Example 1: Country Spatial Analysis")
    print("=" * 80)
    
    countries_data = [
        (1, "United States", "POLYGON((-125 50, -66 50, -66 25, -125 25, -125 50))"),
        (2, "Canada", "POLYGON((-141 70, -52 70, -52 42, -141 42, -141 70))"),
        (3, "Mexico", "POLYGON((-117 33, -86 33, -86 14, -117 14, -117 33))"),
    ]
    
    df = sedona.createDataFrame(countries_data, ["id", "name", "geometry_wkt"])
    df.createOrReplaceTempView("countries")
    
    result_df = sedona.sql("""
        SELECT 
            id,
            name,
            ST_GeomFromText(geometry_wkt) as geometry,
            ST_Area(ST_GeomFromText(geometry_wkt)) as area_sq_degrees,
            ROUND(ST_Area(ST_GeomFromText(geometry_wkt)) * 12321.0, 2) as area_sq_km_approx,
            ST_Length(ST_GeomFromText(geometry_wkt)) as perimeter,
            ST_AsText(ST_Centroid(ST_GeomFromText(geometry_wkt))) as centroid_wkt
        FROM countries
    """)
    
    print("\n📊 Country Statistics:")
    print("-" * 80)
    result_df.select("name", "area_sq_km_approx", "perimeter", "centroid_wkt").show(truncate=False)
    
    # Save to output
    result_df.coalesce(1).write.mode("overwrite").parquet("/workspace/output/countries")
    print("\n✅ Results saved to /workspace/output/countries/")
    
    return result_df


def example_2_spatial_join(sedona):
    """Demonstrate spatial join with Sedona"""
    print("\n" + "=" * 80)
    print("Example 2: Spatial Join (Points within Zones)")
    print("=" * 80)
    
    points_data = [
        (1, "Point A", "POINT(-100 40)"),
        (2, "Point B", "POINT(-70 45)"),
        (3, "Point C", "POINT(-95 30)"),
        (4, "Point D", "POINT(-110 35)"),
    ]
    
    zones_data = [
        ("West", "POLYGON((-125 50, -100 50, -100 25, -125 25, -125 50))"),
        ("East", "POLYGON((-100 50, -66 50, -66 25, -100 25, -100 50))"),
    ]
    
    points_df = sedona.createDataFrame(points_data, ["id", "name", "geometry_wkt"])
    zones_df = sedona.createDataFrame(zones_data, ["zone_name", "geometry_wkt"])
    
    points_df.createOrReplaceTempView("points")
    zones_df.createOrReplaceTempView("zones")
    
    result_df = sedona.sql("""
        SELECT 
            p.id,
            p.name as point_name,
            z.zone_name,
            ST_AsText(ST_GeomFromText(p.geometry_wkt)) as point_location,
            ST_Distance(
                ST_GeomFromText(p.geometry_wkt),
                ST_Centroid(ST_GeomFromText(z.geometry_wkt))
            ) as distance_to_zone_center
        FROM points p
        JOIN zones z ON ST_Within(
            ST_GeomFromText(p.geometry_wkt), 
            ST_GeomFromText(z.geometry_wkt)
        )
        ORDER BY z.zone_name, p.id
    """)
    
    print("\n🔗 Spatial Join Results:")
    print("-" * 80)
    result_df.show(truncate=False)
    
    # Save to output
    result_df.coalesce(1).write.mode("overwrite").parquet("/workspace/output/spatial_joins")
    print("\n✅ Results saved to /workspace/output/spatial_joins/")
    
    return result_df


def example_3_distance_matrix(sedona):
    """Calculate distance matrix between cities"""
    print("\n" + "=" * 80)
    print("Example 3: Distance Matrix Calculation")
    print("=" * 80)
    
    cities_data = [
        ("New York", "POINT(-74.0060 40.7128)"),
        ("Los Angeles", "POINT(-118.2437 34.0522)"),
        ("Chicago", "POINT(-87.6298 41.8781)"),
        ("Houston", "POINT(-95.3698 29.7604)"),
        ("Phoenix", "POINT(-112.0740 33.4484)"),
    ]
    
    cities_df = sedona.createDataFrame(cities_data, ["city", "geometry_wkt"])
    cities_df.createOrReplaceTempView("cities")
    
    result_df = sedona.sql("""
        SELECT 
            c1.city as city1,
            c2.city as city2,
            ST_Distance(
                ST_GeomFromText(c1.geometry_wkt),
                ST_GeomFromText(c2.geometry_wkt)
            ) as distance_degrees,
            ROUND(ST_Distance(
                ST_GeomFromText(c1.geometry_wkt),
                ST_GeomFromText(c2.geometry_wkt)
            ) * 111.32, 2) as distance_km_approx
        FROM cities c1
        CROSS JOIN cities c2
        WHERE c1.city < c2.city
        ORDER BY distance_km_approx DESC
    """)
    
    print("\n📏 City Distances:")
    print("-" * 80)
    result_df.show(truncate=False)
    
    # Save to output
    result_df.coalesce(1).write.mode("overwrite").parquet("/workspace/output/distances")
    print("\n✅ Results saved to /workspace/output/distances/")
    
    return result_df


def example_4_advanced_operations(sedona):
    """Advanced spatial operations"""
    print("\n" + "=" * 80)
    print("Example 4: Advanced Spatial Operations")
    print("=" * 80)
    
    # Create sample geometries
    geoms_data = [
        ("LineString", "LINESTRING(-122.4 37.8, -122.5 37.9, -122.6 37.7)"),
        ("Polygon", "POLYGON((-122.5 37.8, -122.4 37.8, -122.4 37.7, -122.5 37.7, -122.5 37.8))"),
        ("Point", "POINT(-122.45 37.75)"),
    ]
    
    geoms_df = sedona.createDataFrame(geoms_data, ["geom_type", "geometry_wkt"])
    geoms_df.createOrReplaceTempView("geometries")
    
    result_df = sedona.sql("""
        SELECT 
            geom_type,
            ST_AsText(ST_GeomFromText(geometry_wkt)) as original_wkt,
            ST_GeometryType(ST_GeomFromText(geometry_wkt)) as geometry_type,
            ST_IsValid(ST_GeomFromText(geometry_wkt)) as is_valid,
            ST_IsSimple(ST_GeomFromText(geometry_wkt)) as is_simple,
            CASE 
                WHEN geom_type = 'Polygon' THEN ST_Area(ST_GeomFromText(geometry_wkt))
                ELSE NULL 
            END as area,
            CASE 
                WHEN geom_type IN ('LineString', 'Polygon') THEN ST_Length(ST_GeomFromText(geometry_wkt))
                ELSE NULL 
            END as length,
            ST_AsText(ST_Envelope(ST_GeomFromText(geometry_wkt))) as bounding_box,
            ST_AsText(ST_Buffer(ST_GeomFromText(geometry_wkt), 0.1)) as buffer_01deg
        FROM geometries
    """)
    
    print("\n🔧 Advanced Operations:")
    print("-" * 80)
    result_df.show(truncate=False, vertical=True)
    
    # Save to output
    result_df.coalesce(1).write.mode("overwrite").parquet("/workspace/output/advanced_ops")
    print("\n✅ Results saved to /workspace/output/advanced_ops/")
    
    return result_df


def main():
    """Run all Sedona cluster examples"""
    print("\n" + "=" * 80)
    print("🚀 Apache Sedona Distributed Spatial Processing - Cluster Mode")
    print("=" * 80)
    
    sedona = None
    
    try:
        sedona = create_sedona_cluster_context()
        
        # Run all examples
        example_1_country_processing(sedona)
        example_2_spatial_join(sedona)
        example_3_distance_matrix(sedona)
        example_4_advanced_operations(sedona)
        
        print("\n" + "=" * 80)
        print("✅ All Sedona cluster examples completed successfully!")
        print("=" * 80)
        print("\n📊 Output files available in /workspace/output/")
        print("   - countries/")
        print("   - spatial_joins/")
        print("   - distances/")
        print("   - advanced_ops/")
        print("\n🌐 Access Spark UI:")
        print("   - Master UI: http://localhost:8085")
        print("   - Application UI: http://localhost:4040")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
        print("\n💡 If you see dependency errors, Examples 01-03 work perfectly:")
        print("   - python examples/01_simple_geojson.py")
        print("   - python examples/03_spatial_transforms.py")
        raise
    
    finally:
        if sedona:
            sedona.stop()
            print("\n🛑 Spark session stopped")


if __name__ == "__main__":
    main()
