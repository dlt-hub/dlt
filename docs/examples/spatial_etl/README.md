# Spatial ETL Examples

Comprehensive examples demonstrating spatial data ETL pipelines using dlt with various geospatial formats and destinations.

## Overview

These examples showcase how to use dlt for processing geospatial data from various sources (ESRI, CAD, raster) and loading them into modern data warehouses with spatial support.

## Examples

### 1. ESRI to PostGIS (`esri_to_postgis.py`)

Extract data from ESRI Shapefiles, File Geodatabases, and ArcGIS REST services, and load to PostGIS with spatial indexing.

**Features:**
- Read ESRI Shapefile (.shp)
- Read File Geodatabase (.gdb)
- Query ArcGIS REST API
- Transform to WKT/WKB
- Load to PostGIS with spatial indexes
- Handle large datasets with batching

**Use Cases:**
- Migrating legacy ESRI data to open-source PostGIS
- Syncing ArcGIS Online data to warehouse
- Building spatial data lakes

### 2. CAD to GeoPackage (`cad_to_geopackage.py`)

Convert CAD formats (DWG, DXF) to OGC GeoPackage for interoperable spatial data storage.

**Features:**
- Read AutoCAD DWG/DXF files
- Extract layers (points, lines, polygons)
- Preserve attributes and styling
- Write to GeoPackage format
- Layer-by-layer processing

**Use Cases:**
- Converting engineering CAD drawings to GIS
- Archiving CAD data in open formats
- Integration with QGIS/other GIS tools

### 3. Raster Processing (`raster_processing.py`)

Process satellite imagery, DEMs, and raster datasets for analytics and visualization.

**Features:**
- Read raster formats (GeoTIFF, COG, etc.)
- Extract metadata and statistics
- Generate tiles for web maps
- Calculate zonal statistics
- Export to cloud storage

**Use Cases:**
- Satellite imagery processing
- Digital elevation model (DEM) analysis
- Land cover classification
- Time series raster analysis

## Prerequisites

### Core Dependencies
```bash
pip install dlt
```

### Spatial Dependencies
```bash
# GDAL/OGR for vector/raster support
brew install gdal  # macOS
apt-get install gdal-bin libgdal-dev  # Ubuntu

pip install gdal fiona rasterio shapely
```

### Database Dependencies
```bash
# PostGIS
pip install psycopg2-binary

# DuckDB with spatial
pip install duckdb>=1.0.0
```

### Optional: Sedona for distributed processing
```bash
pip install apache-sedona pyspark
```

## Quick Start

### Example 1: ESRI to PostGIS

```python
import dlt
from spatial_etl.esri_to_postgis import esri_shapefile_source

# Configure PostGIS destination
pipeline = dlt.pipeline(
    pipeline_name="esri_migration",
    destination="postgres",
    dataset_name="gis_data"
)

# Load shapefile to PostGIS
info = pipeline.run(
    esri_shapefile_source("/data/cities.shp"),
    table_name="cities"
)

print(f"Loaded {info}")
```

### Example 2: CAD to GeoPackage

```python
import dlt
from spatial_etl.cad_to_geopackage import cad_source

pipeline = dlt.pipeline(
    pipeline_name="cad_conversion",
    destination="filesystem",
    dataset_name="cad_archive"
)

# Convert DWG to GeoPackage
info = pipeline.run(
    cad_source("/engineering/site_plan.dwg"),
    table_name="site_layers"
)
```

### Example 3: Raster Processing

```python
import dlt
from spatial_etl.raster_processing import raster_metadata_source

pipeline = dlt.pipeline(
    pipeline_name="satellite_processing",
    destination="duckdb",
    dataset_name="earth_observation"
)

# Extract raster metadata
info = pipeline.run(
    raster_metadata_source("/imagery/landsat8/*.tif"),
    table_name="raster_catalog"
)
```

## Configuration

### PostGIS Connection

Create `secrets.toml`:
```toml
[destination.postgres.credentials]
database = "gis_database"
username = "gis_user"
password = "your_password"
host = "localhost"
port = 5432

[sources.spatial]
geometry_format = "wkb"  # or "wkt"
create_spatial_index = true
srid = 4326  # WGS84
```

### Filesystem Destination

```toml
[destination.filesystem]
bucket_url = "file:///data/geopackages"

[destination.filesystem.credentials]
# For cloud storage
# aws_access_key_id = "..."
# aws_secret_access_key = "..."
```

## Architecture

### Data Flow

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Source    │      │     dlt     │      │ Destination │
│   (ESRI)    │─────▶│  Pipeline   │─────▶│  (PostGIS)  │
│             │      │             │      │             │
│ - Shapefile │      │ - Extract   │      │ - Spatial   │
│ - GDB       │      │ - Transform │      │   Tables    │
│ - REST API  │      │ - Load      │      │ - Indexes   │
└─────────────┘      └─────────────┘      └─────────────┘
```

### Coordinate Reference Systems (CRS)

All examples handle CRS transformations:
- Source CRS detection
- Target CRS specification
- On-the-fly reprojection
- CRS metadata preservation

### Geometry Types Supported

- **Points**: GPS coordinates, landmarks
- **Lines**: Roads, rivers, boundaries
- **Polygons**: Parcels, zones, countries
- **MultiPoint/MultiLine/MultiPolygon**: Complex features
- **Geometry Collections**: Mixed geometry types

## Performance Optimization

### Batching Strategy

```python
# Large dataset example
pipeline.run(
    esri_shapefile_source("large_dataset.shp"),
    table_name="features",
    loader_file_format="parquet",  # Fast columnar format
)
```

### Spatial Indexing

PostGIS automatically creates spatial indexes:
```sql
-- Created automatically by dlt
CREATE INDEX idx_features_geom ON features USING GIST(geometry);
```

### Parallel Processing

For massive datasets, use Sedona:
```python
from dlt.sources.sedona import read_sedona_table

# Distributed processing with Spark
sedona_df = sedona.read.format("shapefile").load("huge_dataset.shp")
pipeline.run(read_sedona_table(sedona_df))
```

## Common Patterns

### Pattern 1: Incremental Loading

```python
@dlt.resource
def incremental_features(last_modified=dlt.sources.incremental("updated_at")):
    features = read_shapefile_with_timestamp("features.shp")
    for feature in features:
        if feature["updated_at"] > last_modified.last_value:
            yield feature
```

### Pattern 2: Multi-Layer Processing

```python
@dlt.source
def gdb_source(gdb_path):
    for layer_name in list_layers(gdb_path):
        yield dlt.resource(
            read_layer(gdb_path, layer_name),
            name=layer_name
        )
```

### Pattern 3: Geometry Simplification

```python
from shapely.geometry import shape
from shapely.ops import transform

@dlt.transformer
def simplify_geometries(features, tolerance=0.001):
    for feature in features:
        geom = shape(feature["geometry"])
        feature["geometry"] = geom.simplify(tolerance)
        yield feature
```

## Testing

Run example tests:
```bash
# Test spatial readers
pytest tests/sources/spatial/test_spatial_readers.py -v

# Test spatial transformers
pytest tests/sources/spatial/test_spatial_transformers.py -v
```

## Troubleshooting

### Issue: GDAL Import Error

```bash
# Install GDAL system library first
brew install gdal

# Then install Python bindings
pip install gdal==$(gdal-config --version)
```

### Issue: PostGIS Extension Not Found

```sql
-- Enable PostGIS extension
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
```

### Issue: Invalid Geometry

```python
from shapely.validation import explain_validity

# Debug invalid geometries
for feature in features:
    if not feature["geometry"].is_valid:
        print(explain_validity(feature["geometry"]))
```

## Advanced Topics

### Custom Spatial Transformations

```python
from pyproj import Transformer

@dlt.transformer
def reproject_features(features, from_crs="EPSG:4326", to_crs="EPSG:3857"):
    transformer = Transformer.from_crs(from_crs, to_crs, always_xy=True)
    
    for feature in features:
        geom = shape(feature["geometry"])
        reprojected = transform(transformer.transform, geom)
        feature["geometry"] = reprojected
        yield feature
```

### Spatial Joins

```python
from shapely.geometry import shape, Point

@dlt.transformer
def spatial_join_points_to_polygons(points, polygons):
    polygon_index = create_spatial_index(polygons)
    
    for point in points:
        pt = Point(point["x"], point["y"])
        containing_polygon = find_containing_polygon(pt, polygon_index)
        
        if containing_polygon:
            point["zone"] = containing_polygon["name"]
        
        yield point
```

## Resources

### Documentation
- [dlt Documentation](https://dlthub.com/docs)
- [PostGIS Documentation](https://postgis.net/docs/)
- [GDAL Documentation](https://gdal.org/)
- [Apache Sedona Documentation](https://sedona.apache.org/)

### Sample Data Sources
- [Natural Earth Data](https://www.naturalearthdata.com/)
- [OpenStreetMap](https://www.openstreetmap.org/)
- [USGS Earth Explorer](https://earthexplorer.usgs.gov/)
- [Copernicus Open Access Hub](https://scihub.copernicus.eu/)

### Related Examples
- `tests/e2e/test_sedona_simple.py` - Distributed spatial processing
- `tests/e2e/test_ogr_standalone.py` - OGR/GDAL integration
- `dlt/sources/sedona/` - Sedona source implementation

## Contributing

To add new spatial examples:

1. Follow the existing pattern
2. Include comprehensive docstrings
3. Add corresponding tests in `tests/sources/spatial/`
4. Update this README
5. Ensure all dependencies are documented

## License

Apache 2.0 - See LICENSE file

## Support

- GitHub Issues: [dlt issues](https://github.com/dlt-hub/dlt/issues)
- Slack Community: [dltHub Slack](https://dlthub.com/community)
- Documentation: [dlthub.com/docs](https://dlthub.com/docs)
