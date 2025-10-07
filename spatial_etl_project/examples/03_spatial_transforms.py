"""
Example 03: Spatial Transformations

This example demonstrates:
- Buffer creation around points
- CRS transformation (WGS84 to Web Mercator)
- Distance calculations
- Centroid calculation
- Geometry simplification

Requires: shapely, pyproj
"""

import dlt
from typing import Iterator, Dict, Any


@dlt.resource
def cities_with_buffers() -> Iterator[Dict[str, Any]]:
    """Create buffer zones around city points"""
    cities = [
        {"name": "San Francisco", "lon": -122.419, "lat": 37.775},
        {"name": "Los Angeles", "lon": -118.244, "lat": 34.052},
        {"name": "Chicago", "lon": -87.630, "lat": 41.878},
    ]
    
    try:
        from shapely.geometry import Point
        
        for city in cities:
            point = Point(city["lon"], city["lat"])
            
            buffer_05_deg = point.buffer(0.5)
            buffer_1_deg = point.buffer(1.0)
            
            yield {
                "name": city["name"],
                "longitude": city["lon"],
                "latitude": city["lat"],
                "point_wkt": point.wkt,
                "buffer_05deg_wkt": buffer_05_deg.wkt,
                "buffer_1deg_wkt": buffer_1_deg.wkt,
                "buffer_05deg_area": buffer_05_deg.area,
                "buffer_1deg_area": buffer_1_deg.area,
            }
    except ImportError:
        print("⚠️ shapely not installed, using simplified version")
        for city in cities:
            yield {
                "name": city["name"],
                "longitude": city["lon"],
                "latitude": city["lat"],
                "point_wkt": f"POINT({city['lon']} {city['lat']})",
                "buffer_05deg_area": 3.14159 * 0.5 ** 2,
                "buffer_1deg_area": 3.14159 * 1.0 ** 2,
            }


@dlt.resource
def cities_transformed_crs() -> Iterator[Dict[str, Any]]:
    """Transform coordinates from WGS84 to Web Mercator"""
    cities = [
        {"name": "San Francisco", "lon": -122.419, "lat": 37.775},
        {"name": "Los Angeles", "lon": -118.244, "lat": 34.052},
    ]
    
    try:
        from pyproj import Transformer
        
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        
        for city in cities:
            x, y = transformer.transform(city["lon"], city["lat"])
            
            yield {
                "name": city["name"],
                "wgs84_lon": city["lon"],
                "wgs84_lat": city["lat"],
                "web_mercator_x": x,
                "web_mercator_y": y,
                "source_crs": "EPSG:4326",
                "target_crs": "EPSG:3857",
            }
    except ImportError:
        print("⚠️ pyproj not installed, using approximate transformation")
        for city in cities:
            yield {
                "name": city["name"],
                "wgs84_lon": city["lon"],
                "wgs84_lat": city["lat"],
                "web_mercator_x": city["lon"] * 111320,
                "web_mercator_y": city["lat"] * 111320,
                "source_crs": "EPSG:4326",
                "target_crs": "EPSG:3857 (approximate)",
            }


@dlt.resource
def distance_matrix() -> Iterator[Dict[str, Any]]:
    """Calculate distances between city pairs"""
    cities = [
        {"name": "San Francisco", "lon": -122.419, "lat": 37.775},
        {"name": "Los Angeles", "lon": -118.244, "lat": 34.052},
        {"name": "Chicago", "lon": -87.630, "lat": 41.878},
    ]
    
    try:
        from shapely.geometry import Point
        
        for i, city1 in enumerate(cities):
            for city2 in cities[i + 1:]:
                p1 = Point(city1["lon"], city1["lat"])
                p2 = Point(city2["lon"], city2["lat"])
                
                distance = p1.distance(p2)
                
                yield {
                    "city1": city1["name"],
                    "city2": city2["name"],
                    "distance_degrees": distance,
                    "distance_km_approx": distance * 111,
                }
    except ImportError:
        import math
        for i, city1 in enumerate(cities):
            for city2 in cities[i + 1:]:
                dx = city2["lon"] - city1["lon"]
                dy = city2["lat"] - city1["lat"]
                distance = math.sqrt(dx**2 + dy**2)
                
                yield {
                    "city1": city1["name"],
                    "city2": city2["name"],
                    "distance_degrees": distance,
                    "distance_km_approx": distance * 111,
                }


def main():
    """Run spatial transformation examples"""
    print("=" * 80)
    print("Example 03: Spatial Transformations")
    print("=" * 80)
    
    pipeline = dlt.pipeline(
        pipeline_name="spatial_transforms",
        destination="duckdb",
        dataset_name="spatial_examples"
    )
    
    print("\n[1/3] Creating buffer zones around cities...")
    info1 = pipeline.run(
        cities_with_buffers(),
        table_name="cities_buffers"
    )
    print(f"✅ Loaded {info1.load_packages[0].state} - cities_buffers")
    
    print("\n[2/3] Transforming CRS (WGS84 → Web Mercator)...")
    info2 = pipeline.run(
        cities_transformed_crs(),
        table_name="cities_web_mercator"
    )
    print(f"✅ Loaded {info2.load_packages[0].state} - cities_web_mercator")
    
    print("\n[3/3] Calculating distance matrix...")
    info3 = pipeline.run(
        distance_matrix(),
        table_name="city_distances"
    )
    print(f"✅ Loaded {info3.load_packages[0].state} - city_distances")
    
    print("\n" + "=" * 80)
    print("✅ All transformations completed successfully!")
    print("=" * 80)
    print("""
Query the data:

    import duckdb
    conn = duckdb.connect('spatial_transforms.duckdb')
    
    # View buffers
    conn.execute("SELECT name, buffer_05deg_area FROM spatial_examples.cities_buffers").fetchall()
    
    # View CRS transformations
    conn.execute("SELECT * FROM spatial_examples.cities_web_mercator").fetchall()
    
    # View distances
    conn.execute('''
        SELECT city1, city2, distance_km_approx 
        FROM spatial_examples.city_distances 
        ORDER BY distance_km_approx
    ''').fetchall()
    """)


if __name__ == "__main__":
    main()
