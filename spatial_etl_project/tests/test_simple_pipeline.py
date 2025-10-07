"""
Simple pipeline tests - no optional dependencies required
"""

import pytest
import dlt
from pathlib import Path


def test_geojson_pipeline():
    """Test basic GeoJSON pipeline"""
    
    @dlt.resource
    def sample_cities():
        return [
            {"name": "San Francisco", "lat": 37.775, "lon": -122.419},
            {"name": "Los Angeles", "lat": 34.052, "lon": -118.244},
        ]
    
    pipeline = dlt.pipeline(
        pipeline_name="test_geojson",
        destination="duckdb",
        dataset_name="test_spatial"
    )
    
    info = pipeline.run(sample_cities(), table_name="test_cities")
    
    assert len(info.load_packages) > 0
    assert info.load_packages[0].state == "loaded"


def test_spatial_transformations():
    """Test spatial transformations with shapely"""
    try:
        from shapely.geometry import Point
        
        point = Point(-122.419, 37.775)
        buffered = point.buffer(0.5)
        
        assert buffered.area > 0
        assert point.wkt.startswith("POINT")
        
    except ImportError:
        pytest.skip("shapely not installed")


def test_buffer_calculation():
    """Test buffer calculation"""
    try:
        from shapely.geometry import Point
        
        @dlt.resource
        def cities_with_buffers():
            cities = [
                {"name": "SF", "lon": -122.419, "lat": 37.775},
                {"name": "LA", "lon": -118.244, "lat": 34.052},
            ]
            
            for city in cities:
                point = Point(city["lon"], city["lat"])
                buffer = point.buffer(0.5)
                
                yield {
                    "name": city["name"],
                    "point_wkt": point.wkt,
                    "buffer_wkt": buffer.wkt,
                    "buffer_area": buffer.area,
                }
        
        pipeline = dlt.pipeline(
            pipeline_name="test_buffers",
            destination="duckdb",
            dataset_name="test_spatial"
        )
        
        info = pipeline.run(cities_with_buffers(), table_name="test_buffers")
        
        assert len(info.load_packages) > 0
        assert info.load_packages[0].state == "loaded"
        
    except ImportError:
        pytest.skip("shapely not installed")


def test_distance_calculation():
    """Test distance calculation"""
    try:
        from shapely.geometry import Point
        
        p1 = Point(0, 0)
        p2 = Point(3, 4)
        
        distance = p1.distance(p2)
        
        assert abs(distance - 5.0) < 0.01
        
    except ImportError:
        import math
        dx = 3 - 0
        dy = 4 - 0
        distance = math.sqrt(dx**2 + dy**2)
        assert abs(distance - 5.0) < 0.01


def test_crs_transformation():
    """Test CRS transformation"""
    try:
        from pyproj import Transformer
        
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        
        x, y = transformer.transform(-122.419, 37.775)
        
        assert x < 0
        assert y > 0
        assert abs(x) > 10000
        
    except ImportError:
        pytest.skip("pyproj not installed")


def test_project_structure():
    """Test that project structure is correct"""
    project_root = Path(__file__).parent.parent
    
    assert (project_root / "README.md").exists()
    assert (project_root / "requirements.txt").exists()
    assert (project_root / "examples").exists()
    assert (project_root / "data").exists()
    assert (project_root / ".vscode").exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
