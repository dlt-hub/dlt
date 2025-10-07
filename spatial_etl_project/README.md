# Spatial ETL Project - Complete Working Examples

A ready-to-run VS Code project demonstrating spatial ETL pipelines with dlt.

## Project Structure

```
spatial_etl_project/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment variables template
├── docker-compose.yml           # PostGIS + Sedona services
├── run_sedona_docker.sh         # Run Sedona in Docker
├── SEDONA_DOCKER_GUIDE.md       # Docker setup guide
├── .vscode/
│   ├── settings.json           # VS Code configuration
│   ├── launch.json             # Debug configurations
│   └── tasks.json              # Build/test tasks
├── data/
│   ├── shapefiles/             # Shapefile test data
│   ├── geojson/                # GeoJSON test data
│   ├── cad/                    # CAD file examples
│   ├── raster/                 # Raster/imagery data
│   └── output/                 # Pipeline outputs
├── examples/
│   ├── 01_simple_geojson.py    # Simple GeoJSON pipeline
│   ├── 02_shapefile_reader.py  # Shapefile reader
│   ├── 03_spatial_transforms.py # Spatial transformations
│   ├── 04_sedona_distributed.py # Distributed (local - has issues)
│   ├── 04_sedona_docker.py     # Distributed (Docker - works!)
│   ├── 05_esri_to_postgis.py   # ESRI to PostGIS (full)
│   ├── 06_cad_to_geopackage.py # CAD to GeoPackage (full)
│   └── 07_raster_processing.py # Raster processing (full)
├── tests/
│   ├── test_simple_pipeline.py # Simple test examples
│   └── test_all_examples.py    # Test all examples work
```

## Quick Start

### 1. Install Dependencies

```bash
# Core dependencies (required)
pip install -r requirements.txt

# Optional: GDAL (for Shapefile/Raster support)
brew install gdal  # macOS
# or
conda install -c conda-forge gdal

# Optional: PostGIS support
pip install psycopg2-binary
```

### 2. Set Environment Variables

```bash
cp .env.example .env
# Edit .env with your configuration
```

### 3. Start PostGIS (Optional)

```bash
docker-compose up -d
```

### 4. Run Examples

#### Simple GeoJSON Example (No dependencies required)
```bash
python examples/01_simple_geojson.py
```

#### Spatial Transformations
```bash
python examples/03_spatial_transforms.py
```

#### Distributed Processing with Sedona

**⚠️ Note**: Local Sedona setup has version compatibility issues. Use Docker instead:

```bash
# Run with Docker (recommended)
./run_sedona_docker.sh

# Or use docker-compose
docker-compose up sedona-spark
```

See [SEDONA_DOCKER_GUIDE.md](SEDONA_DOCKER_GUIDE.md) for details.

#### Full ESRI to PostGIS Pipeline
```bash
python examples/05_esri_to_postgis.py
```

### 5. Run Tests

```bash
# All tests
pytest tests/ -v

# Specific test
pytest tests/test_simple_pipeline.py -v
```

## VS Code Features

### Debug Configurations

Press `F5` or use the Debug panel to run:
- **Debug Current Example** - Run the currently open example file
- **Debug All Tests** - Run pytest with debugging
- **Debug Specific Example** - Choose from menu

### Tasks (Cmd/Ctrl + Shift + B)

- **Run Core Tests** - Run tests without optional dependencies
- **Run All Tests** - Run complete test suite
- **Format Code** - Format with black
- **Lint Code** - Check with pylint

### Keyboard Shortcuts

- `F5` - Start debugging
- `Cmd/Ctrl + Shift + B` - Run build task
- `Cmd/Ctrl + Shift + P` - Command palette
- `Cmd/Ctrl + Shift + '` - Open terminal

## Example Usage

### 1. Simple GeoJSON Pipeline

```python
import dlt

@dlt.resource
def cities():
    return [
        {"name": "San Francisco", "lat": 37.775, "lon": -122.419},
        {"name": "Los Angeles", "lat": 34.052, "lon": -118.244},
    ]

pipeline = dlt.pipeline(
    pipeline_name="simple_spatial",
    destination="duckdb",
    dataset_name="cities"
)

info = pipeline.run(cities())
print(f"Loaded {len(info.load_packages)} packages")
```

### 2. Spatial Transformations

```python
from shapely.geometry import Point
from shapely import wkt

# Create point and buffer
point = Point(-122.419, 37.775)
buffered = point.buffer(0.1)

# Transform to WKT
geometry_wkt = buffered.wkt
print(f"Buffer: {geometry_wkt}")
```

### 3. Distributed Processing with Sedona

**⚠️ Use Docker for Sedona** (see [SEDONA_DOCKER_GUIDE.md](SEDONA_DOCKER_GUIDE.md)):

```bash
# Run Sedona examples in Docker
./run_sedona_docker.sh
```

**Docker version** (`04_sedona_docker.py`):
```python
from sedona.spark import SedonaContext

sedona = SedonaContext.builder() \
    .appName("spatial-etl") \
    .master("local[*]") \
    .getOrCreate()

# Use Sedona SQL for spatial operations
result = sedona.sql("""
    SELECT ST_Area(ST_GeomFromText(geometry)) as area
    FROM spatial_data
""")

# Process spatial data at scale
df = spark.createDataFrame([
    (1, "POINT(-122.419 37.775)"),
], ["id", "geometry"])

df.createOrReplaceTempView("points")
result = spark.sql("""
    SELECT id, ST_AsText(ST_GeomFromText(geometry)) as wkt
    FROM points
""")
result.show()
```

## Sample Data

### Shapefiles
- `ne_110m_admin_0_countries.shp` - World countries (Natural Earth)
- `point.shp` - Point features test data
- `poly.shp` - Polygon features test data

### GeoJSON
- `cities.geojson` - Major US cities
- `states.geojson` - US state boundaries

### Outputs
All pipeline outputs are saved to `data/output/`

## Testing

### Run Core Tests (No GDAL/PostGIS required)
```bash
pytest tests/test_simple_pipeline.py -v
```

### Run All Tests (Requires all dependencies)
```bash
pytest tests/ -v
```

### Test Individual Examples
```bash
python examples/01_simple_geojson.py
python examples/03_spatial_transforms.py
python examples/04_sedona_distributed.py
```

## Dependencies Status

### ✅ Required (Always installed)
- `dlt` - Core ETL framework
- `apache-sedona>=1.5.0` - Distributed spatial
- `pyspark>=3.3.0` - Spark engine
- `shapely>=2.0.0` - Geometry operations

### ⏭️ Optional (Graceful fallback)
- `gdal` - Shapefile/Raster support
- `psycopg2-binary` - PostGIS support
- `rasterio` - Advanced raster processing

## Troubleshooting

### "GDAL not found"
```bash
# Install GDAL
brew install gdal  # macOS
conda install -c conda-forge gdal  # conda

# Set environment variables
export GDAL_LIBRARY_PATH=/opt/homebrew/lib/libgdal.dylib
export GEOS_LIBRARY_PATH=/opt/homebrew/lib/libgeos_c.dylib
```

### "PostGIS connection failed"
```bash
# Start Docker PostGIS
docker-compose up -d

# Check status
docker ps | grep postgis

# Test connection
psql postgresql://postgres:postgres@localhost:5432/postgres
```

### Architecture mismatch (macOS)
```bash
# Check Python architecture
python -c "import platform; print(platform.machine())"

# Use Homebrew Python (ARM64)
/opt/homebrew/bin/python3 -m pip install gdal

# Or use conda-forge
conda install -c conda-forge gdal psycopg2
```

## Documentation

- **Installation Guide**: See `docs/examples/spatial_etl/INSTALLATION_GUIDE.md`
- **API Documentation**: See `docs/examples/spatial_etl/README.md`
- **Test Documentation**: See `tests/README.md`

## Support

For issues or questions:
1. Check VS Code "Problems" panel for errors
2. Run tests: `pytest tests/ -v`
3. Check logs in `data/output/logs/`
4. Review installation guide

## License

MIT License - See parent dlt project for details

---

**Last Updated**: 2025-10-07  
**dlt Version**: Latest  
**Apache Sedona**: 1.6.0  
**Python**: 3.9+
