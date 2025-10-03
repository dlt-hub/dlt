"""Tests for spatial transformers"""

import pytest

try:
    from dlt.sources.spatial.transformers import (
        _reproject,
        _buffer_geometry,
        _spatial_filter,
        _validate_geometry,
        _simplify_geometry,
        _geometry_extractor,
    )
    from shapely import wkt
    SPATIAL_AVAILABLE = True
except ImportError:
    SPATIAL_AVAILABLE = False


@pytest.mark.skipif(not SPATIAL_AVAILABLE, reason="Spatial dependencies not installed")
class TestSpatialTransformers:
    """Test suite for spatial transformers"""

    def test_reproject_transformer(self):
        """Test coordinate reprojection"""
        features = [
            {
                'id': 1,
                'geometry': 'POINT(2.3522 48.8566)',
            }
        ]

        result = list(_reproject(
            iter(features),
            source_crs='EPSG:4326',
            target_crs='EPSG:3857'
        ))

        assert len(result) == 1
        assert '_reprojected' in result[0]
        assert result[0]['_reprojected'] is True
        assert 'geometry' in result[0]

        geom = wkt.loads(result[0]['geometry'])
        assert geom.x != 2.3522

    def test_buffer_geometry_transformer(self):
        """Test geometry buffering"""
        features = [
            {
                'id': 1,
                'geometry': 'POINT(0 0)',
            }
        ]

        result = list(_buffer_geometry(
            iter(features),
            distance=10.0
        ))

        assert len(result) == 1
        assert '_buffered' in result[0]
        assert result[0]['_buffer_distance'] == 10.0

        geom = wkt.loads(result[0]['geometry'])
        assert geom.geom_type == 'Polygon'

    def test_spatial_filter_intersects(self):
        """Test spatial filter with intersects predicate"""
        features = [
            {
                'id': 1,
                'geometry': 'POINT(0 0)',
            },
            {
                'id': 2,
                'geometry': 'POINT(10 10)',
            }
        ]

        filter_geometry = 'POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))'

        result = list(_spatial_filter(
            iter(features),
            filter_geometry=filter_geometry,
            predicate='intersects'
        ))

        assert len(result) == 1
        assert result[0]['id'] == 1

    def test_validate_geometry(self):
        """Test geometry validation"""
        valid_features = [
            {
                'id': 1,
                'geometry': 'POINT(0 0)',
            }
        ]

        result = list(_validate_geometry(iter(valid_features)))

        assert len(result) == 1
        assert result[0]['_geometry_valid'] is True

    def test_simplify_geometry(self):
        """Test geometry simplification"""
        features = [
            {
                'id': 1,
                'geometry': 'LINESTRING(0 0, 1 1, 2 0, 3 1, 4 0)',
            }
        ]

        result = list(_simplify_geometry(
            iter(features),
            tolerance=0.5
        ))

        assert len(result) == 1
        assert '_simplified' in result[0]
        assert result[0]['_simplification_tolerance'] == 0.5

    def test_geometry_extractor_centroid(self):
        """Test extracting centroid from geometry"""
        features = [
            {
                'id': 1,
                'geometry': 'POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))',
            }
        ]

        result = list(_geometry_extractor(
            iter(features),
            extract_centroid=True,
            extract_area=True
        ))

        assert len(result) == 1
        assert '_centroid_x' in result[0]
        assert '_centroid_y' in result[0]
        assert '_area' in result[0]
        assert result[0]['_centroid_x'] == 2.0
        assert result[0]['_centroid_y'] == 2.0
        assert result[0]['_area'] == 16.0

    def test_geometry_extractor_bounds(self):
        """Test extracting bounds from geometry"""
        features = [
            {
                'id': 1,
                'geometry': 'LINESTRING(1 1, 5 5)',
            }
        ]

        result = list(_geometry_extractor(
            iter(features),
            extract_bounds=True,
            extract_length=True
        ))

        assert len(result) == 1
        assert '_bounds' in result[0]
        assert '_length' in result[0]
        assert result[0]['_bounds']['minx'] == 1.0
        assert result[0]['_bounds']['maxx'] == 5.0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
