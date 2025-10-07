"""
Example 01: Simple GeoJSON Pipeline

This example demonstrates:
- Reading GeoJSON data
- Loading into DuckDB
- Basic spatial queries

No optional dependencies required (GDAL, PostGIS not needed)
"""

import dlt
import json
from pathlib import Path
from typing import Iterator, Dict, Any


@dlt.resource
def geojson_cities() -> Iterator[Dict[str, Any]]:
    """Read cities from GeoJSON file"""
    geojson_path = Path("data/geojson/cities.geojson")
    
    if not geojson_path.exists():
        print(f"❌ GeoJSON file not found: {geojson_path}")
        print("Creating sample data...")
        geojson_path.parent.mkdir(parents=True, exist_ok=True)
        sample_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "San Francisco", "population": 873965},
                    "geometry": {"type": "Point", "coordinates": [-122.419, 37.775]}
                },
                {
                    "type": "Feature",
                    "properties": {"name": "Los Angeles", "population": 3979576},
                    "geometry": {"type": "Point", "coordinates": [-118.244, 34.052]}
                },
            ]
        }
        with open(geojson_path, 'w') as f:
            json.dump(sample_data, f, indent=2)
    
    with open(geojson_path, 'r') as f:
        data = json.load(f)
    
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [None, None])
        
        yield {
            "name": props.get("name"),
            "state": props.get("state"),
            "population": props.get("population"),
            "founded": props.get("founded"),
            "longitude": coords[0],
            "latitude": coords[1],
            "geometry_type": geom.get("type"),
            "geometry_json": json.dumps(geom)
        }


def main():
    """Run the GeoJSON pipeline"""
    print("=" * 80)
    print("Example 01: Simple GeoJSON Pipeline")
    print("=" * 80)
    
    pipeline = dlt.pipeline(
        pipeline_name="geojson_cities",
        destination="duckdb",
        dataset_name="spatial_examples"
    )
    
    info = pipeline.run(
        geojson_cities(),
        table_name="cities"
    )
    
    print(f"\n✅ Pipeline completed successfully!")
    print(f"   - Loaded {len(info.load_packages)} package(s)")
    print(f"   - Destination: {info.destination_type}")
    print(f"   - Dataset: {info.dataset_name}")
    print(f"   - State: {info.load_packages[0].state}")
    
    print("\n" + "=" * 80)
    print("Query the data using DuckDB:")
    print("=" * 80)
    print("""
    import duckdb
    
    conn = duckdb.connect('geojson_cities.duckdb')
    
    # View all cities
    conn.execute("SELECT * FROM spatial_examples.cities").fetchall()
    
    # Cities by population
    conn.execute('''
        SELECT name, population 
        FROM spatial_examples.cities 
        ORDER BY population DESC
    ''').fetchall()
    """)
    
    return info


if __name__ == "__main__":
    info = main()
