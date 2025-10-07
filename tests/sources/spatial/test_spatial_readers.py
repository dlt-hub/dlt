"""
Tests for spatial data readers

Tests various spatial file format readers and data sources
including shapefiles, GeoJSON, CAD files, and raster data.
"""

import pytest
from pathlib import Path
from typing import List, Dict, Any
import tempfile
import os

import dlt
from dlt.common.exceptions import MissingDependencyException


TEST_DATA_DIR = Path(__file__).parent.parent.parent / "e2e" / "data" / "spatial"


@pytest.fixture
def temp_shapefile():
    """Create a temporary test shapefile"""
    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = Path(tmpdir) / "test.shp"
        
        try:
            from osgeo import ogr, osr
            
            driver = ogr.GetDriverByName("ESRI Shapefile")
            ds = driver.CreateDataSource(str(shp_path))
            
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(4326)
            
            layer = ds.CreateLayer("test_layer", srs, ogr.wkbPoint)
            
            field_defn = ogr.FieldDefn("name", ogr.OFTString)
            layer.CreateField(field_defn)
            
            field_defn = ogr.FieldDefn("value", ogr.OFTInteger)
            layer.CreateField(field_defn)
            
            points = [
                ("Point 1", -122.419, 37.775, 100),
                ("Point 2", -118.244, 34.052, 200),
                ("Point 3", -87.630, 41.878, 300),
            ]
            
            for name, lon, lat, value in points:
                feature = ogr.Feature(layer.GetLayerDefn())
                feature.SetField("name", name)
                feature.SetField("value", value)
                
                point = ogr.Geometry(ogr.wkbPoint)
                point.AddPoint(lon, lat)
                feature.SetGeometry(point)
                
                layer.CreateFeature(feature)
                feature = None
            
            ds = None
            
            yield str(shp_path)
            
        except ImportError:
            pytest.skip("GDAL not installed")


def test_shapefile_reader_basic():
    """Test basic shapefile reading functionality"""
    try:
        from osgeo import ogr
    except ImportError:
        pytest.skip("GDAL not installed")
    
    @dlt.resource
    def read_shapefile(path: str):
        driver = ogr.GetDriverByName("ESRI Shapefile")
        ds = driver.Open(path, 0)
        
        if ds is None:
            raise ValueError(f"Cannot open shapefile: {path}")
        
        layer = ds.GetLayer()
        
        for feature in layer:
            geom = feature.GetGeometryRef()
            
            yield {
                "name": feature.GetField("NAME") if "NAME" in [
                    feature.GetFieldDefnRef(i).GetName() 
                    for i in range(feature.GetFieldCount())
                ] else "Unknown",
                "geometry_wkt": geom.ExportToWkt() if geom else None,
                "geometry_type": geom.GetGeometryName() if geom else None
            }
        
        ds = None
    
    shp_file = TEST_DATA_DIR / "poly.shp"
    
    if not shp_file.exists():
        pytest.skip("Test shapefile not found")
    
    features = list(read_shapefile(str(shp_file)))
    
    assert len(features) > 0
    assert all("geometry_wkt" in f for f in features)
    assert all(f["geometry_type"] is not None for f in features)


def test_shapefile_with_dlt_pipeline(temp_shapefile):
    """Test shapefile reading with complete dlt pipeline"""
    try:
        from osgeo import ogr
    except ImportError:
        pytest.skip("GDAL not installed")
    
    @dlt.resource
    def shapefile_source(path: str):
        driver = ogr.GetDriverByName("ESRI Shapefile")
        ds = driver.Open(path, 0)
        layer = ds.GetLayer()
        
        for feature in layer:
            geom = feature.GetGeometryRef()
            
            yield {
                "name": feature.GetField("name"),
                "value": feature.GetField("value"),
                "geometry": geom.ExportToWkb() if geom else None,
                "lon": geom.GetX() if geom else None,
                "lat": geom.GetY() if geom else None
            }
        
        ds = None
    
    pipeline = dlt.pipeline(
        pipeline_name="test_shapefile_reader",
        destination="duckdb",
        dataset_name="test_spatial"
    )
    
    info = pipeline.run(
        shapefile_source(temp_shapefile),
        table_name="test_points"
    )
    
    assert len(info.load_packages) > 0
    assert info.load_packages[0].state == "loaded"


def test_geojson_reader():
    """Test GeoJSON format reading"""
    import json
    
    @dlt.resource
    def geojson_source(geojson_data: Dict[str, Any]):
        for feature in geojson_data.get("features", []):
            props = feature.get("properties", {})
            geom = feature.get("geometry")
            
            yield {
                **props,
                "geometry_type": geom.get("type") if geom else None,
                "coordinates": str(geom.get("coordinates")) if geom else None
            }
    
    test_geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"name": "Test Point", "value": 42},
                "geometry": {
                    "type": "Point",
                    "coordinates": [-122.4, 37.8]
                }
            }
        ]
    }
    
    features = list(geojson_source(test_geojson))
    
    assert len(features) == 1
    assert features[0]["name"] == "Test Point"
    assert features[0]["geometry_type"] == "Point"


def test_wkt_geometry_reader():
    """Test reading geometries from WKT strings"""
    
    @dlt.resource
    def wkt_source():
        wkt_geometries = [
            ("POINT(-122.419 37.775)", "San Francisco"),
            ("POINT(-118.244 34.052)", "Los Angeles"),
            ("LINESTRING(-122.4 37.8, -118.2 34.0)", "Route")
        ]
        
        for wkt, name in wkt_geometries:
            try:
                from shapely import wkt as wkt_parser
                geom = wkt_parser.loads(wkt)
                
                yield {
                    "name": name,
                    "wkt": wkt,
                    "geometry_type": geom.geom_type,
                    "is_valid": geom.is_valid,
                    "bounds": list(geom.bounds) if geom.bounds else None
                }
            except ImportError:
                yield {
                    "name": name,
                    "wkt": wkt,
                    "geometry_type": "Unknown",
                    "is_valid": True,
                    "bounds": None
                }
    
    features = list(wkt_source())
    
    assert len(features) == 3
    assert all("wkt" in f for f in features)
    assert features[0]["name"] == "San Francisco"


def test_coordinate_transformation():
    """Test coordinate reference system transformation"""
    try:
        from osgeo import osr
    except ImportError:
        pytest.skip("GDAL not installed")
    
    source_srs = osr.SpatialReference()
    source_srs.ImportFromEPSG(4326)
    
    target_srs = osr.SpatialReference()
    target_srs.ImportFromEPSG(3857)
    
    transform = osr.CoordinateTransformation(source_srs, target_srs)
    
    lon, lat = -122.419, 37.775
    
    x, y, z = transform.TransformPoint(lat, lon)
    
    assert x != lon
    assert y != lat
    assert abs(x) > 10000


def test_batch_reading():
    """Test batch reading of spatial features"""
    
    def generate_batched_features():
        batch_size = 10
        batch = []
        
        for i in range(25):
            feature = {
                "id": i,
                "name": f"Feature {i}",
                "geometry": f"POINT({i} {i})"
            }
            
            batch.append(feature)
            
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        if batch:
            yield batch
    
    all_batches = list(generate_batched_features())
    
    assert len(all_batches) == 3
    assert len(all_batches[0]) == 10
    assert len(all_batches[1]) == 10
    assert len(all_batches[2]) == 5


def test_spatial_filter():
    """Test spatial filtering (bounding box)"""
    
    @dlt.resource
    def filtered_features(bbox: tuple):
        min_x, min_y, max_x, max_y = bbox
        
        all_points = [
            (-122.0, 37.0, "Inside"),
            (-130.0, 40.0, "Outside"),
            (-121.0, 36.0, "Inside"),
            (-100.0, 30.0, "Outside"),
        ]
        
        for x, y, label in all_points:
            if min_x <= x <= max_x and min_y <= y <= max_y:
                yield {
                    "x": x,
                    "y": y,
                    "label": label
                }
    
    bbox = (-123.0, 36.0, -120.0, 38.0)
    features = list(filtered_features(bbox))
    
    assert len(features) == 2
    assert all(f["label"] == "Inside" for f in features)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
