# Spatial ETL Installation Guide

This guide covers installation of optional dependencies for the dlt spatial ETL framework.

---

## Core Dependencies (Required)

These are already installed and working:

```bash
pip install dlt
pip install apache-sedona>=1.5.0
pip install pyspark>=3.3.0
pip install shapely>=2.0.0
```

✅ **Status**: All core dependencies installed and validated

---

## Optional Dependencies

### GDAL/OGR (for Shapefile, Raster, CAD formats)

#### Option 1: Homebrew + Python Bindings (Recommended for ARM64 macOS)

```bash
# Install GDAL system library
brew install gdal

# Set library paths (add to ~/.zshrc or ~/.bash_profile)
export GDAL_LIBRARY_PATH=/opt/homebrew/lib/libgdal.dylib
export GEOS_LIBRARY_PATH=/opt/homebrew/lib/libgeos_c.dylib

# Install Python bindings (requires matching architecture)
pip install gdal==$(gdal-config --version)
```

**Note**: If you see architecture mismatch errors (`building for macOS-x86_64 but attempting to link with file built for macOS-arm64`), your Python installation architecture doesn't match your system libraries.

**Solutions**:
1. Use Homebrew Python: `brew install python@3.9 && /opt/homebrew/bin/python3 -m pip install gdal`
2. Use conda-forge ARM64: `conda install -c conda-forge gdal`
3. Use Rosetta x86_64 Homebrew: `arch -x86_64 brew install gdal`

#### Option 2: Conda/Mamba (Cross-platform)

```bash
conda install -c conda-forge gdal
# or
mamba install -c conda-forge gdal
```

#### Option 3: Docker (Most Reliable)

```bash
docker run -it --rm \
  -v $(pwd):/workspace \
  osgeo/gdal:alpine-normal-latest \
  python3 -m pip install dlt
```

---

### PostgreSQL/PostGIS (for PostGIS integration)

#### Option 1: psycopg2-binary (Precompiled - Easiest)

```bash
pip install psycopg2-binary
```

**Note**: If compilation fails, you likely have an architecture mismatch. Try:

```bash
# For ARM64 macOS with Homebrew Python
brew install postgresql
/opt/homebrew/bin/python3 -m pip install psycopg2-binary

# For conda
conda install -c conda-forge psycopg2
```

#### Option 2: psycopg2 from Source (Advanced)

```bash
# Install PostgreSQL client libraries
brew install postgresql  # macOS
# or
sudo apt-get install libpq-dev  # Ubuntu/Debian

# Install Python package
pip install psycopg2
```

#### Option 3: Docker PostgreSQL/PostGIS

```bash
# Start PostGIS container
docker run -d \
  --name postgis \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgis/postgis:latest

# Install Python client
pip install psycopg2-binary

# Test connection
python3 -c "import psycopg2; conn = psycopg2.connect('postgresql://postgres:postgres@localhost:5432/postgres'); print('✅ Connected')"
```

---

## Architecture Compatibility Issues

### Problem: x86_64 Python with ARM64 Libraries (Apple Silicon)

**Symptoms**:
```
building for macOS-x86_64 but attempting to link with file built for macOS-arm64
```

**Diagnosis**:
```bash
# Check Python architecture
python3 -c "import platform; print(platform.machine())"
# x86_64 = Intel/Rosetta
# arm64 = Apple Silicon native

# Check system architecture
uname -m
# arm64 = Apple Silicon
```

**Solutions**:

1. **Use ARM64 Python** (Recommended):
```bash
# Install Homebrew ARM64 Python
brew install python@3.9

# Use Homebrew Python
/opt/homebrew/bin/python3 -m pip install gdal psycopg2-binary
```

2. **Use x86_64 Homebrew** (via Rosetta):
```bash
# Install x86_64 Homebrew
arch -x86_64 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install GDAL via x86_64 Homebrew
arch -x86_64 /usr/local/bin/brew install gdal

# Install Python bindings
export GDAL_LIBRARY_PATH=/usr/local/lib/libgdal.dylib
pip install gdal
```

3. **Use conda-forge** (Handles architecture automatically):
```bash
conda install -c conda-forge gdal psycopg2
```

---

## Environment Variables

Add these to your `~/.zshrc` or `~/.bash_profile`:

```bash
# GDAL library paths (for macOS Homebrew ARM64)
export GDAL_LIBRARY_PATH=/opt/homebrew/lib/libgdal.dylib
export GEOS_LIBRARY_PATH=/opt/homebrew/lib/libgeos_c.dylib

# PostgreSQL (if needed)
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
```

For dlt configuration, create `.dlt/config.toml`:

```toml
[sources.spatial]
gdal_library_path = "/opt/homebrew/lib/libgdal.dylib"
geos_library_path = "/opt/homebrew/lib/libgeos_c.dylib"

[destination.postgres.credentials]
database = "postgres"
username = "postgres"
password = "postgres"
host = "localhost"
port = 5432
```

---

## Verification

### Test GDAL Installation

```python
try:
    from osgeo import gdal, ogr, osr
    print(f"✅ GDAL installed: {gdal.__version__}")
except ImportError as e:
    print(f"❌ GDAL not installed: {e}")
```

### Test PostGIS Connection

```python
try:
    import psycopg2
    conn = psycopg2.connect(
        "postgresql://postgres:postgres@localhost:5432/postgres"
    )
    cur = conn.cursor()
    cur.execute("SELECT PostGIS_version();")
    print(f"✅ PostGIS connected: {cur.fetchone()}")
except Exception as e:
    print(f"❌ PostGIS not available: {e}")
```

### Run Spatial Tests

```bash
# All tests (will skip if dependencies unavailable)
pytest tests/sources/spatial/ -v

# GDAL-specific tests
pytest tests/sources/spatial/test_spatial_readers.py -v

# PostGIS-specific tests
pytest tests/sources/spatial/test_postgis_integration.py -v
```

---

## Graceful Degradation

**Good news**: All spatial ETL tests are designed to skip gracefully when optional dependencies are unavailable. You'll see:

```
SKIPPED [1] test_spatial_readers.py:75: GDAL not installed
SKIPPED [1] test_postgis_integration.py:39: psycopg2 not installed
```

This is **not a failure** - it's intentional graceful degradation. Core spatial functionality works without these dependencies:

- ✅ GeoJSON parsing
- ✅ WKT/WKB geometry handling
- ✅ Apache Sedona distributed processing
- ✅ Shapely geometry operations
- ✅ Spatial transformations (buffer, centroid, distance, etc.)

Only Shapefile/Raster reading and PostGIS loading require the optional dependencies.

---

## Platform-Specific Notes

### macOS Apple Silicon (M1/M2/M3)

**Issue**: Architecture mismatch between x86_64 Python (Anaconda) and ARM64 libraries (Homebrew)

**Best Solution**: Use Homebrew ARM64 Python
```bash
brew install python@3.9
/opt/homebrew/bin/python3 -m pip install dlt gdal psycopg2-binary
```

### macOS Intel

```bash
brew install gdal postgresql
pip install gdal psycopg2-binary
```

### Linux (Ubuntu/Debian)

```bash
sudo apt-get update
sudo apt-get install gdal-bin libgdal-dev libpq-dev
pip install gdal psycopg2-binary
```

### Linux (RHEL/CentOS)

```bash
sudo yum install gdal gdal-devel postgresql-devel
pip install gdal psycopg2-binary
```

### Windows

```bash
# Use conda (recommended)
conda install -c conda-forge gdal psycopg2

# Or use OSGeo4W
# Download from: https://trac.osgeo.org/osgeo4w/
```

---

## Docker Development Environment (Recommended)

For the most reliable development environment:

```dockerfile
FROM python:3.9-slim

# Install GDAL and PostgreSQL client
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install dlt apache-sedona pyspark shapely gdal psycopg2-binary

WORKDIR /workspace
```

Build and run:
```bash
docker build -t dlt-spatial .
docker run -it --rm -v $(pwd):/workspace dlt-spatial bash
```

---

## Troubleshooting

### Error: "GDAL not found"
```bash
# Check system GDAL
which gdalinfo
gdal-config --version

# Set environment variables
export GDAL_LIBRARY_PATH=/opt/homebrew/lib/libgdal.dylib
```

### Error: "Could not connect to PostgreSQL"
```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Or use Docker
docker ps | grep postgres
```

### Error: "Architecture mismatch"
```bash
# Check Python architecture
python3 -c "import platform; print(platform.machine())"

# Use matching architecture Python
# ARM64: /opt/homebrew/bin/python3
# x86_64: /usr/local/bin/python3 or Anaconda python
```

---

## Support

If you encounter issues:

1. **Check test output**: Tests provide clear skip messages
2. **Verify architecture**: Ensure Python and system libraries match
3. **Use Docker**: Most reliable cross-platform solution
4. **Graceful degradation**: Core functionality works without optional deps

---

**Last Updated**: 2025-10-07  
**dlt Version**: Latest  
**Sedona Version**: 1.6.0+  
**Tested Platforms**: macOS (ARM64 + Intel), Linux (Ubuntu), Docker
