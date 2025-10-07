# Quick Start Guide

Get up and running with the Spatial ETL project in 5 minutes.

## Prerequisites

- Python 3.9+
- VS Code (recommended)
- Docker (optional, for PostGIS)

## 1. Setup Project

```bash
cd spatial_etl_project

# Create virtual environment (optional but recommended)
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env
```

## 2. Run Your First Example

```bash
# Example 01: Simple GeoJSON (no dependencies required)
python examples/01_simple_geojson.py
```

**Expected output:**
```
================================================================================
Example 01: Simple GeoJSON Pipeline
================================================================================

✅ Pipeline completed successfully!
   - Loaded 1 package(s)
   - Destination: dlt.destinations.duckdb
   - Dataset: spatial_examples
   - State: loaded
```

## 3. Run Spatial Transformations

```bash
# Example 03: Spatial transformations (requires shapely)
python examples/03_spatial_transforms.py
```

This will:
- Create buffer zones around cities
- Transform CRS (WGS84 → Web Mercator)
- Calculate distance matrix

## 4. Run Distributed Processing

```bash
# Example 04: Sedona distributed (requires apache-sedona)
python examples/04_sedona_distributed.py
```

## 5. Run Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test
pytest tests/test_simple_pipeline.py -v
```

## VS Code Quick Actions

### Open in VS Code
```bash
code .
```

### Debug Example (F5)
1. Open any example file (e.g., `examples/01_simple_geojson.py`)
2. Press `F5` or click "Run and Debug"
3. Choose "Debug Current Example"

### Run Tasks (Cmd/Ctrl + Shift + B)
- **Run Core Tests** - Run tests without optional dependencies
- **Run All Tests** - Run complete test suite
- **Start PostGIS** - Start Docker PostgreSQL/PostGIS

## Project Structure

```
spatial_etl_project/
├── examples/               # Ready-to-run examples
│   ├── 01_simple_geojson.py
│   ├── 02_shapefile_reader.py
│   ├── 03_spatial_transforms.py
│   ├── 04_sedona_distributed.py
│   ├── 05_esri_to_postgis.py
│   ├── 06_cad_to_geopackage.py
│   └── 07_raster_processing.py
├── data/                   # Sample datasets
│   ├── geojson/           # GeoJSON test data
│   ├── shapefiles/        # Shapefile test data
│   └── output/            # Pipeline outputs
├── tests/                  # Test suite
└── .vscode/               # VS Code config
```

## Example Outputs

All examples save data to DuckDB:

```python
import duckdb

# Connect to example output
conn = duckdb.connect('geojson_cities.duckdb')

# Query the data
conn.execute("SELECT * FROM spatial_examples.cities").fetchall()
```

## Optional: Start PostGIS

For examples that need PostGIS (05, 06):

```bash
# Start PostgreSQL/PostGIS
docker-compose up -d

# Verify it's running
docker ps | grep postgis

# Install Python client
pip install psycopg2-binary
```

## Next Steps

1. ✅ **Explore examples** - Try all 7 examples in sequence
2. ✅ **Modify examples** - Adapt them to your data
3. ✅ **Run tests** - Understand how the code works
4. ✅ **Read documentation** - Check `README.md` for details

## Common Issues

### "GDAL not found"
Examples 02, 05, 06, 07 need GDAL for shapefile/raster support.

**Solution**: Examples will use sample data if GDAL not available.

To install GDAL:
```bash
# macOS
brew install gdal

# Linux
sudo apt-get install gdal-bin libgdal-dev

# Python bindings
pip install gdal
```

### "PostGIS connection failed"
Examples 05 needs PostGIS.

**Solution**: Start Docker PostGIS:
```bash
docker-compose up -d
pip install psycopg2-binary
```

### Import errors
Make sure dependencies are installed:
```bash
pip install -r requirements.txt
```

## Help

- 📖 **Full documentation**: `README.md`
- 🔧 **Installation guide**: `../docs/examples/spatial_etl/INSTALLATION_GUIDE.md`
- 🧪 **Run tests**: `pytest tests/ -v`

---

**Ready to go!** Start with `examples/01_simple_geojson.py` 🚀
