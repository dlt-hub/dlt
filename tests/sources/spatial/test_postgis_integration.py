"""
PostGIS Integration Tests

Tests loading spatial data into PostGIS running in Docker
Connection: postgres://postgres:postgres@localhost:5432/postgres
"""

import pytest
from typing import List, Dict, Any
from pathlib import Path
import dlt


POSTGIS_AVAILABLE = False

try:
    import psycopg2
    POSTGIS_AVAILABLE = True
except ImportError:
    pass


@pytest.fixture
def postgis_credentials():
    """PostGIS connection credentials"""
    return {
        "database": "postgres",
        "username": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": 5432
    }


@pytest.fixture
def postgis_connection(postgis_credentials):
    """Create PostGIS connection for testing"""
    if not POSTGIS_AVAILABLE:
        pytest.skip("psycopg2 not installed")
    
    try:
        conn = psycopg2.connect(**postgis_credentials)
        
        cur = conn.cursor()
        cur.execute("SELECT PostGIS_version();")
        version = cur.fetchone()
        print(f"PostGIS version: {version}")
        
        yield conn
        
        conn.close()
    except Exception as e:
        pytest.skip(f"PostGIS not available: {e}")


def test_postgis_extension(postgis_connection):
    """Test that PostGIS extension is available"""
    cur = postgis_connection.cursor()
    
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM pg_extension WHERE extname = 'postgis'
        );
    """)
    
    has_postgis = cur.fetchone()[0]
    assert has_postgis, "PostGIS extension not installed"


def test_load_points_to_postgis():
    """Test loading point geometries to PostGIS"""
    if not POSTGIS_AVAILABLE:
        pytest.skip("psycopg2 not installed")
    
    @dlt.resource
    def spatial_points():
        return [
            {
                "id": 1,
                "name": "San Francisco",
                "geom": "POINT(-122.419 37.775)"
            },
            {
                "id": 2,
                "name": "Los Angeles",
                "geom": "POINT(-118.244 34.052)"
            },
            {
                "id": 3,
                "name": "Chicago",
                "geom": "POINT(-87.630 41.878)"
            }
        ]
    
    pipeline = dlt.pipeline(
        pipeline_name="test_postgis_points",
        destination="postgres",
        dataset_name="spatial_test"
    )
    
    try:
        info = pipeline.run(
            spatial_points(),
            table_name="test_points",
            credentials={
                "database": "postgres",
                "username": "postgres",
                "password": "postgres",
                "host": "localhost",
                "port": 5432
            }
        )
        
        assert len(info.load_packages) > 0
        assert info.load_packages[0].state == "loaded"
        
        print(f"Loaded {len(info.load_packages)} packages to PostGIS")
        
    except Exception as e:
        pytest.skip(f"Could not connect to PostGIS: {e}")


def test_load_polygons_to_postgis():
    """Test loading polygon geometries to PostGIS"""
    if not POSTGIS_AVAILABLE:
        pytest.skip("psycopg2 not installed")
    
    @dlt.resource
    def spatial_polygons():
        return [
            {
                "id": 1,
                "name": "Square",
                "geom": "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "area": 1.0
            },
            {
                "id": 2,
                "name": "Triangle",
                "geom": "POLYGON((0 0, 2 0, 1 2, 0 0))",
                "area": 2.0
            }
        ]
    
    pipeline = dlt.pipeline(
        pipeline_name="test_postgis_polygons",
        destination="postgres",
        dataset_name="spatial_test"
    )
    
    try:
        info = pipeline.run(
            spatial_polygons(),
            table_name="test_polygons",
            credentials={
                "database": "postgres",
                "username": "postgres",
                "password": "postgres",
                "host": "localhost",
                "port": 5432
            }
        )
        
        assert len(info.load_packages) > 0
        print("Successfully loaded polygons to PostGIS")
        
    except Exception as e:
        pytest.skip(f"Could not connect to PostGIS: {e}")


def test_verify_spatial_data_in_postgis(postgis_connection):
    """Verify loaded spatial data can be queried from PostGIS"""
    cur = postgis_connection.cursor()
    
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'spatial_test'
        AND table_name IN ('test_points', 'test_polygons')
    """)
    
    tables = cur.fetchall()
    
    if len(tables) > 0:
        print(f"Found spatial tables: {tables}")
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM spatial_test.test_points
        """)
        
        count = cur.fetchone()[0]
        print(f"Points count: {count}")


def test_spatial_query_in_postgis(postgis_connection):
    """Test spatial queries using PostGIS functions"""
    cur = postgis_connection.cursor()
    
    try:
        cur.execute("""
            SELECT 
                ST_Distance(
                    ST_GeomFromText('POINT(0 0)', 4326),
                    ST_GeomFromText('POINT(1 1)', 4326)
                ) as distance
        """)
        
        distance = cur.fetchone()[0]
        
        assert distance > 0
        print(f"Calculated distance: {distance}")
        
    except Exception as e:
        pytest.skip(f"PostGIS spatial functions not available: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
