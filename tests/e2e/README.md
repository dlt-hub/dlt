# E2E Tests for Spatial ETL

## Overview
This directory contains end-to-end tests for spatial ETL integrations:
- **Sedona tests**: Distributed spatial processing with Apache Sedona + PySpark
- **OGR/GDAL tests**: Direct GDAL/OGR spatial data reading (no Sedona required)

Both test suites use browser automation to verify data in the dlt dashboard UI.

## Test Data
Spatial test datasets are stored in `tests/e2e/data/spatial/`:
- **poly.shp**: GDAL polygon test dataset
- **ne_110m_admin_0_countries.shp**: Natural Earth world countries dataset (110m scale)

## Running the Tests

### Prerequisites - Sedona Tests
1. Install Sedona dependencies:
   ```bash
   pip install apache-sedona>=1.5.0 pyspark>=3.3.0
   ```

2. Note: Playwright tests require greenlet which has compilation issues on macOS 15 SDK.
   Skip Playwright installation for now and run non-UI tests only.

### Prerequisites - OGR/GDAL Tests
1. Install GDAL system library:
   ```bash
   brew install gdal  # macOS
   ```

2. Install Python GDAL:
   ```bash
   pip install gdal
   ```

### Run Tests (Without Playwright)
```bash
# Run Sedona test fixtures only
pytest tests/e2e/test_sedona_spatial.py::test_sedona_spatial_pipeline --co

# Run OGR test fixtures only  
pytest tests/e2e/test_ogr_spatial.py::test_ogr_shapefile_pipeline --co
```

### To Run with Playwright (when greenlet compilation is fixed)
```bash
pip install playwright pytest-playwright
playwright install chromium
pytest tests/e2e/test_sedona_spatial.py -v
pytest tests/e2e/test_ogr_spatial.py -v
```

## Test Cases

### Sedona Tests (test_sedona_spatial.py)

#### test_sedona_spatial_pipeline
Tests basic Sedona spatial data loading:
- Reads Natural Earth countries shapefile with Sedona
- Calculates spatial metrics (area, perimeter) using ST_Area/ST_Length
- Loads to DuckDB via dlt
- Verifies data in dashboard UI

#### test_sedona_spatial_join_pipeline
Tests advanced Sedona spatial SQL queries:
- Executes spatial SQL with Sedona functions (ST_Area, ST_Centroid, ST_Within)
- Performs spatial analysis (categorization by area)
- Validates geometry transformations
- Checks query results in dashboard

### OGR/GDAL Tests (test_ogr_spatial.py)

#### test_ogr_shapefile_pipeline
Tests GDAL/OGR shapefile reading:
- Uses osgeo.ogr to read polygon shapefile
- Extracts geometry as WKT
- Calculates area using OGR methods
- Loads to DuckDB without Sedona

#### test_ogr_countries_pipeline  
Tests batch processing with GDAL/OGR:
- Reads Natural Earth countries with pure OGR
- Processes in batches of 50 features
- Extracts centroids and attributes
- Validates in dashboard UI

## Architecture

### Sedona Approach
- **Scale**: Millions to billions of features (distributed)
- **Processing**: Spark/Flink distributed computing
- **Functions**: 300+ Sedona spatial functions
- **Use case**: Large-scale spatial analytics

### OGR/GDAL Approach
- **Scale**: Thousands to millions of features (single machine)
- **Processing**: Direct file reading with GDAL
- **Functions**: Native OGR geometry methods
- **Use case**: Standard GIS file formats

## Data Sources
- GDAL test data: https://github.com/OSGeo/gdal/tree/master/autotest/ogr/data
- Natural Earth data: https://www.naturalearthdata.com/downloads/
