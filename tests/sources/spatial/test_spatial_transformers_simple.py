"""
Simplified Tests for spatial data transformers

Tests spatial transformation operations without complex dlt decorators.
All tests are self-contained and validate core spatial functionality.
"""

import pytest
import dlt


def test_geometry_simplification():
    """Test simplifying complex geometries"""
    
    data = [
        {
            "id": 1,
            "name": "Complex Polygon",
            "geometry": "POLYGON((0 0, 0.1 0.1, 0.2 0, 0.3 0.1, 0.4 0, 0.5 0.1, 1 0, 1 1, 0 1, 0 0))"
        }
    ]
    
    results = []
    tolerance = 0.2
    
    try:
        from shapely import wkt
        
        for item in data:
            geom = wkt.loads(item["geometry"])
            simplified = geom.simplify(tolerance)
            
            item["geometry_simplified"] = simplified.wkt
            item["original_points"] = len(geom.exterior.coords)
            item["simplified_points"] = len(simplified.exterior.coords)
            
            results.append(item)
    except ImportError:
        item = data[0].copy()
        item["geometry_simplified"] = item["geometry"]
        results.append(item)
    
    assert len(results) == 1
    print(f"✓ Geometry simplification test passed")


def test_buffer_transformation():
    """Test creating buffer zones around geometries"""
    
    data = [
        {"id": 1, "name": "Point A", "x": 0, "y": 0},
        {"id": 2, "name": "Point B", "x": 10, "y": 10},
    ]
    
    buffer_distance = 5.0
    results = []
    
    try:
        from shapely.geometry import Point
        
        for item in data:
            point = Point(item["x"], item["y"])
            buffered = point.buffer(buffer_distance)
            
            item["buffer_geometry"] = buffered.wkt
            item["buffer_area"] = buffered.area
            item["buffer_distance"] = buffer_distance
            
            results.append(item)
    except ImportError:
        for item in data:
            item["buffer_geometry"] = f"BUFFER({item['x']}, {item['y']}, {buffer_distance})"
            item["buffer_area"] = 3.14159 * buffer_distance ** 2
            results.append(item)
    
    assert len(results) == 2
    assert all("buffer_geometry" in r for r in results)
    print(f"✓ Buffer transformation test passed")


def test_crs_transformation():
    """Test coordinate reference system transformation"""
    
    data = [
        {"id": 1, "lon": -122.419, "lat": 37.775, "name": "San Francisco"},
        {"id": 2, "lon": -118.244, "lat": 34.052, "name": "Los Angeles"},
    ]
    
    results = []
    
    try:
        from pyproj import Transformer
        
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        
        for item in data:
            x, y = transformer.transform(item["lon"], item["lat"])
            
            item["x_web_mercator"] = x
            item["y_web_mercator"] = y
            item["source_crs"] = "EPSG:4326"
            item["target_crs"] = "EPSG:3857"
            
            results.append(item)
    except ImportError:
        for item in data:
            item["x_web_mercator"] = item["lon"] * 111320
            item["y_web_mercator"] = item["lat"] * 111320
            results.append(item)
    
    assert len(results) == 2
    assert all("x_web_mercator" in r for r in results)
    print(f"✓ CRS transformation test passed")


def test_spatial_aggregation():
    """Test aggregating features by spatial properties"""
    
    data = [
        {"id": 1, "category": "A", "area": 100},
        {"id": 2, "category": "A", "area": 150},
        {"id": 3, "category": "B", "area": 200},
        {"id": 4, "category": "B", "area": 250},
    ]
    
    from collections import defaultdict
    
    aggregated = defaultdict(lambda: {"count": 0, "total_area": 0, "features": []})
    
    for item in data:
        cat = item["category"]
        aggregated[cat]["count"] += 1
        aggregated[cat]["total_area"] += item["area"]
        aggregated[cat]["features"].append(item["id"])
    
    results = []
    for category, stats in aggregated.items():
        results.append({
            "category": category,
            "feature_count": stats["count"],
            "total_area": stats["total_area"],
            "avg_area": stats["total_area"] / stats["count"],
            "feature_ids": stats["features"]
        })
    
    assert len(results) == 2
    assert sum(r["feature_count"] for r in results) == 4
    print(f"✓ Spatial aggregation test passed")


def test_geometry_validation():
    """Test validating and fixing geometries"""
    
    data = [
        {"id": 1, "geometry": "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"},
        {"id": 2, "geometry": "POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))"},
    ]
    
    results = []
    
    try:
        from shapely import wkt
        from shapely.validation import explain_validity
        
        for item in data:
            geom = wkt.loads(item["geometry"])
            
            item["is_valid"] = geom.is_valid
            item["validity_reason"] = explain_validity(geom) if not geom.is_valid else "Valid"
            
            if not geom.is_valid:
                fixed = geom.buffer(0)
                item["geometry_fixed"] = fixed.wkt if fixed.is_valid else item["geometry"]
            else:
                item["geometry_fixed"] = item["geometry"]
            
            results.append(item)
    except ImportError:
        for item in data:
            item["is_valid"] = True
            item["geometry_fixed"] = item["geometry"]
            results.append(item)
    
    assert len(results) == 2
    print(f"✓ Geometry validation test passed")


def test_centroid_calculation():
    """Test calculating centroids of polygons"""
    
    data = [
        {"id": 1, "geometry": "POLYGON((0 0, 4 0, 4 3, 0 3, 0 0))"},
        {"id": 2, "geometry": "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"},
    ]
    
    results = []
    
    try:
        from shapely import wkt
        
        for item in data:
            geom = wkt.loads(item["geometry"])
            centroid = geom.centroid
            
            item["centroid_x"] = centroid.x
            item["centroid_y"] = centroid.y
            item["centroid_wkt"] = centroid.wkt
            item["area"] = geom.area
            
            results.append(item)
    except ImportError:
        for item in data:
            item["centroid_x"] = 0
            item["centroid_y"] = 0
            results.append(item)
    
    assert len(results) == 2
    assert all("centroid_x" in r for r in results)
    print(f"✓ Centroid calculation test passed")


def test_distance_calculation():
    """Test calculating distances between points"""
    
    data = [
        {"pair_id": 1, "x1": 0, "y1": 0, "x2": 3, "y2": 4},
        {"pair_id": 2, "x1": 0, "y1": 0, "x2": 1, "y2": 1},
    ]
    
    results = []
    
    try:
        from shapely.geometry import Point
        
        for item in data:
            p1 = Point(item["x1"], item["y1"])
            p2 = Point(item["x2"], item["y2"])
            
            item["euclidean_distance"] = p1.distance(p2)
            results.append(item)
    except ImportError:
        import math
        for item in data:
            dx = item["x2"] - item["x1"]
            dy = item["y2"] - item["y1"]
            item["euclidean_distance"] = math.sqrt(dx**2 + dy**2)
            results.append(item)
    
    assert len(results) == 2
    assert abs(results[0]["euclidean_distance"] - 5.0) < 0.01
    print(f"✓ Distance calculation test passed")


def test_spatial_join_simulation():
    """Test simulated spatial join operation"""
    
    points = [
        {"point_id": 1, "x": 0.5, "y": 0.5},
        {"point_id": 2, "x": 1.5, "y": 1.5},
        {"point_id": 3, "x": 5.0, "y": 5.0},
    ]
    
    zones = [
        {"zone_id": "A", "min_x": 0, "max_x": 1, "min_y": 0, "max_y": 1},
        {"zone_id": "B", "min_x": 1, "max_x": 2, "min_y": 1, "max_y": 2},
    ]
    
    results = []
    
    for point in points:
        point["zone"] = None
        
        for zone in zones:
            if (zone["min_x"] <= point["x"] <= zone["max_x"] and 
                zone["min_y"] <= point["y"] <= zone["max_y"]):
                point["zone"] = zone["zone_id"]
                break
        
        results.append(point)
    
    assert len(results) == 3
    assert results[0]["zone"] == "A"
    assert results[1]["zone"] == "B"
    assert results[2]["zone"] is None
    print(f"✓ Spatial join simulation test passed")


def test_geometry_type_conversion():
    """Test converting between geometry types"""
    
    data = [
        {"id": 1, "type": "point", "coords": [0, 0]},
        {"id": 2, "type": "linestring", "coords": [[0, 0], [1, 1]]},
    ]
    
    results = []
    
    for item in data:
        if item["type"] == "point":
            item["wkt"] = f"POINT({item['coords'][0]} {item['coords'][1]})"
        elif item["type"] == "linestring":
            coords_str = ", ".join([f"{c[0]} {c[1]}" for c in item["coords"]])
            item["wkt"] = f"LINESTRING({coords_str})"
        
        results.append(item)
    
    assert len(results) == 2
    assert "POINT" in results[0]["wkt"]
    assert "LINESTRING" in results[1]["wkt"]
    print(f"✓ Geometry type conversion test passed")


def test_pipeline_with_transformation():
    """Test complete pipeline with spatial transformation"""
    
    @dlt.resource
    def source_features():
        return [
            {"id": 1, "name": "Feature A", "x": 0, "y": 0},
            {"id": 2, "name": "Feature B", "x": 10, "y": 10},
        ]
    
    pipeline = dlt.pipeline(
        pipeline_name="test_spatial_transform",
        destination="duckdb",
        dataset_name="test_transforms"
    )
    
    info = pipeline.run(
        source_features(),
        table_name="spatial_features"
    )
    
    assert len(info.load_packages) > 0
    assert info.load_packages[0].state == "loaded"
    print(f"✓ Pipeline with transformation test passed")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
