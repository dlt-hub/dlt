"""
Example 04: Distributed Spatial Processing with Apache Sedona (Docker Version)

This is a standalone version designed to run in the official Apache Sedona Docker image.
It demonstrates Sedona's distributed spatial processing without requiring dlt.

Run with Docker:
    docker-compose up sedona-spark

Or run standalone in Sedona container:
    docker run -v $(pwd):/workspace apache/sedona:1.6.0 \
        spark-submit --master local[*] \
        --packages org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.0 \
        /workspace/examples/04_sedona_docker.py
"""

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext


def create_sedona_context():
    """Create Sedona-enabled Spark session"""
    config = SparkSession.builder \
        .appName("Sedona-Spatial-ETL") \
        .master("local[*]") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
        .getOrCreate()
    
    sedona = SedonaContext.create(config)
    sedona.sparkContext.setLogLevel("WARN")
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
            ST_Area(ST_GeomFromText(geometry_wkt)) as area,
            ST_Length(ST_GeomFromText(geometry_wkt)) as perimeter,
            ST_AsText(ST_Centroid(ST_GeomFromText(geometry_wkt))) as centroid_wkt
        FROM countries
    """)
    
    print("\n📊 Country Statistics:")
    print("-" * 80)
    result_df.select("name", "area", "perimeter", "centroid_wkt").show(truncate=False)
    
    return result_df


def example_2_spatial_join(sedona):
    """Demonstrate spatial join"""
    print("\n" + "=" * 80)
    print("Example 2: Spatial Join (Points within Zones)")
    print("=" * 80)
    
    points_data = [
        (1, "Point A", "POINT(-100 40)"),
        (2, "Point B", "POINT(-70 45)"),
        (3, "Point C", "POINT(-95 30)"),
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
            ST_AsText(ST_GeomFromText(p.geometry_wkt)) as point_location
        FROM points p
        JOIN zones z ON ST_Within(
            ST_GeomFromText(p.geometry_wkt), 
            ST_GeomFromText(z.geometry_wkt)
        )
    """)
    
    print("\n🔗 Spatial Join Results:")
    print("-" * 80)
    result_df.show(truncate=False)
    
    return result_df


def example_3_distance_matrix(sedona):
    """Calculate distance matrix"""
    print("\n" + "=" * 80)
    print("Example 3: Distance Matrix Calculation")
    print("=" * 80)
    
    cities_data = [
        ("New York", "POINT(-74.0060 40.7128)"),
        ("Los Angeles", "POINT(-118.2437 34.0522)"),
        ("Chicago", "POINT(-87.6298 41.8781)"),
        ("Houston", "POINT(-95.3698 29.7604)"),
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
        ORDER BY distance_degrees DESC
    """)
    
    print("\n📏 City Distances:")
    print("-" * 80)
    result_df.show(truncate=False)
    
    return result_df


def main():
    """Run all Sedona examples"""
    print("\n" + "=" * 80)
    print("🚀 Apache Sedona Distributed Spatial Processing")
    print("=" * 80)
    
    sedona = create_sedona_context()
    
    try:
        example_1_country_processing(sedona)
        example_2_spatial_join(sedona)
        example_3_distance_matrix(sedona)
        
        print("\n" + "=" * 80)
        print("✅ All Sedona examples completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        sedona.stop()


if __name__ == "__main__":
    main()
