"""
Example 02: Shapefile Reader

This example demonstrates:
- Reading shapefiles with GDAL/OGR
- Extracting geometry and attributes
- Loading into DuckDB
- Graceful fallback if GDAL not available

Optional: GDAL/OGR (will use sample data if not available)
"""

import dlt
from pathlib import Path
from typing import Iterator, Dict, Any


@dlt.resource
def shapefile_features(shapefile_path: str) -> Iterator[Dict[str, Any]]:
    """Read features from shapefile"""
    try:
        from osgeo import ogr
        
        driver = ogr.GetDriverByName("ESRI Shapefile")
        datasource = driver.Open(shapefile_path, 0)
        
        if not datasource:
            raise ValueError(f"Cannot open shapefile: {shapefile_path}")
        
        layer = datasource.GetLayer()
        print(f"✅ Reading {layer.GetFeatureCount()} features from {Path(shapefile_path).name}")
        
        for feature in layer:
            geom = feature.GetGeometryRef()
            
            attributes = {}
            for i in range(feature.GetFieldCount()):
                field_name = feature.GetFieldDefnRef(i).GetName()
                attributes[field_name] = feature.GetField(i)
            
            yield {
                **attributes,
                "geometry_wkt": geom.ExportToWkt() if geom else None,
                "geometry_type": geom.GetGeometryName() if geom else None,
                "area": geom.GetArea() if geom and hasattr(geom, 'GetArea') else None,
            }
        
        datasource = None
        
    except ImportError:
        print("⚠️ GDAL not installed - using sample data")
        yield {
            "NAME": "Sample Feature",
            "geometry_wkt": "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
            "geometry_type": "POLYGON",
            "area": 1.0,
        }
    except Exception as e:
        print(f"⚠️ Error reading shapefile: {e}")
        yield {
            "error": str(e),
            "shapefile": shapefile_path,
        }


def main():
    """Run shapefile reader example"""
    print("=" * 80)
    print("Example 02: Shapefile Reader")
    print("=" * 80)
    
    shapefile_path = "data/shapefiles/poly.shp"
    
    if not Path(shapefile_path).exists():
        print(f"\n⚠️ Shapefile not found: {shapefile_path}")
        print("Using sample data instead...")
        shapefile_path = "sample.shp"
    
    pipeline = dlt.pipeline(
        pipeline_name="shapefile_reader",
        destination="duckdb",
        dataset_name="spatial_examples"
    )
    
    print(f"\n📂 Reading shapefile: {shapefile_path}")
    
    info = pipeline.run(
        shapefile_features(shapefile_path),
        table_name="shapefile_features"
    )
    
    print(f"\n✅ Pipeline completed successfully!")
    print(f"   - State: {info.load_packages[0].state}")
    print(f"   - Destination: {info.destination_type}")
    
    print("\n" + "=" * 80)
    print("Query the data:")
    print("=" * 80)
    print("""
    import duckdb
    
    conn = duckdb.connect('shapefile_reader.duckdb')
    
    # View all features
    conn.execute("SELECT * FROM spatial_examples.shapefile_features").fetchall()
    
    # Count by geometry type
    conn.execute('''
        SELECT geometry_type, COUNT(*) as count
        FROM spatial_examples.shapefile_features
        GROUP BY geometry_type
    ''').fetchall()
    """)


if __name__ == "__main__":
    main()
