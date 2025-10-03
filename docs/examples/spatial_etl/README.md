# Spatial ETL with dlt - Open Source Alternative to FME

This directory contains examples demonstrating dlt's comprehensive spatial ETL capabilities using OGR/GDAL. These examples show how to use dlt as a powerful, open-source alternative to proprietary tools like FME Software.

## Overview

dlt's spatial module provides:

- **170+ Vector Formats Support**: ESRI (Shapefile, FileGDB), CAD (DWG, DXF, DGN), GeoJSON, KML, PostGIS, and more
- **200+ Raster Formats Support**: GeoTIFF, NetCDF, HDF5, ECW, JPEG2000, and more
- **Spatial Transformations**: Reproject, buffer, clip, simplify, spatial join, and more
- **Multiple Destinations**: PostGIS, GeoPackage, DuckDB, Parquet, and 25+ other destinations

## Installation

```bash
# Full spatial support
pip install 'dlt[spatial]'

# Spatial + PostgreSQL/PostGIS
pip install 'dlt[spatial,postgres]'

# Spatial + GeoPackage
pip install 'dlt[spatial,geopackage]'
```

## Examples

### 1. ESRI FileGDB to PostGIS (`esri_to_postgis.py`)

Convert ESRI FileGDB data to PostGIS with coordinate reprojection and buffering.

```python
import dlt
from dlt.sources.spatial import read_vector, reproject, buffer_geometry

pipeline = dlt.pipeline(
    pipeline_name='esri_to_postgis',
    destination='postgres',
    dataset_name='spatial_data'
)

roads = read_vector('/data/city.gdb', layer_name='Roads')

roads_transformed = (
    roads
    | reproject(source_crs='EPSG:2154', target_crs='EPSG:4326')
    | buffer_geometry(distance=10)
)

pipeline.run(roads_transformed, table_name='roads_buffered')
```

**Comparable to FME**: Reader (ESRI Geodatabase) → Reprojector → Bufferer → Writer (PostGIS)

### 2. CAD to GeoPackage (`cad_to_geopackage.py`)

Convert AutoCAD DWG/DXF files to OGC GeoPackage format.

```python
from dlt.sources.spatial import read_vector, attribute_mapper

cad_features = read_vector('/data/building.dwg', layer_name='0')

transformed = (
    cad_features
    | reproject(source_crs='EPSG:3857', target_crs='EPSG:4326')
    | attribute_mapper(mapping={'Layer': 'layer_name', 'Color': 'color_code'})
)

pipeline.run(transformed, table_name='cad_features')
```

**Comparable to FME**: Reader (Autodesk AutoCAD DWG/DXF) → Reprojector → AttributeRenamer → Writer (GeoPackage)

### 3. Raster Processing (`raster_processing.py`)

Process GeoTIFF elevation data and satellite imagery.

```python
from dlt.sources.spatial import read_raster

elevation_data = read_raster(
    file_path='/data/dem.tif',
    bands=[1],
    target_crs='EPSG:4326',
    resample_method='cubic'
)

pipeline.run(elevation_data, table_name='elevation_tiles')
```

**Comparable to FME**: Reader (GeoTIFF) → RasterResampler → RasterReprojector → Writer (PostgreSQL)

## Supported Spatial Formats

### Vector Formats (via OGR)

- **ESRI**: Shapefile, FileGDB, PersonalGDB, ArcSDE
- **CAD**: AutoCAD DWG, DXF, MicroStation DGN
- **Open Standards**: GeoJSON, GeoPackage, KML, GML, GPX
- **Databases**: PostGIS, Oracle Spatial, SQL Server Spatial, MySQL Spatial
- **Other**: MapInfo TAB, OpenStreetMap, TopoJSON, FlatGeobuf, Parquet

### Raster Formats (via GDAL)

- **Common**: GeoTIFF, Cloud Optimized GeoTIFF (COG), NetCDF, HDF5
- **Imagery**: ECW, MrSID, JPEG2000, NITF
- **Elevation**: DEM, SRTM, ASTER
- **Scientific**: Zarr, GRIB

## Spatial Transformers

dlt provides FME-style transformers for spatial operations:

| dlt Transformer | FME Equivalent | Description |
|----------------|----------------|-------------|
| `reproject` | Reprojector | Change coordinate systems |
| `buffer_geometry` | Bufferer | Create buffer zones |
| `spatial_filter` | SpatialFilter | Filter by spatial relationship |
| `validate_geometry` | GeometryValidator | Validate and repair geometries |
| `simplify_geometry` | Generalizer | Simplify complex geometries |
| `spatial_join` | PointOnAreaOverlayer | Spatial join operations |
| `attribute_mapper` | AttributeRenamer/Manager | Map and transform attributes |
| `geometry_extractor` | GeometryPropertyExtractor | Extract geometric properties |

## Destinations

Spatial data can be loaded to:

- **PostGIS** (PostgreSQL + spatial extension)
- **GeoPackage** (OGC standard SQLite)
- **DuckDB** (with spatial extension)
- **Parquet** (with WKT/WKB geometry)
- **Snowflake, BigQuery, Redshift** (geometry as text/binary)
- And 20+ other destinations

## Key Advantages Over FME

1. **Open Source**: Apache 2.0 license, free to use
2. **Python Native**: Full programmability and extensibility
3. **Cloud Native**: Direct S3, GCS, Azure integration
4. **Scalable**: Built-in parallelization and streaming
5. **Version Control**: Pipelines as code
6. **Modern Stack**: Works with dbt, Airflow, Dagster
7. **No License Costs**: FME Desktop costs $5,000+/year
8. **Community Driven**: Active development and support

## Advanced Usage

### Incremental Loading

```python
from dlt.sources import incremental

@dlt.resource(primary_key="feature_id")
def spatial_incremental():
    features = read_vector(
        '/data/updates.gdb',
        attribute_filter="modified_date > '2024-01-01'"
    )
    return features | reproject(target_crs='EPSG:4326')

pipeline.run(spatial_incremental())
```

### Spatial Join Example

```python
from dlt.sources.spatial import spatial_join

buildings = read_vector('/data/buildings.shp')
zones = list(read_vector('/data/zones.shp'))

buildings_with_zones = buildings | spatial_join(
    join_features=zones,
    predicate='within',
    join_attributes=['zone_name', 'zone_id']
)

pipeline.run(buildings_with_zones)
```

### Batch Processing

```python
from pathlib import Path

for shapefile in Path('/data/shapefiles').glob('*.shp'):
    features = read_vector(str(shapefile))
    pipeline.run(features, table_name=shapefile.stem)
```

## Performance Tips

1. **Chunk Size**: Adjust `chunk_size` parameter for optimal memory usage
2. **Spatial Indexing**: Enable spatial indexes in PostGIS/GeoPackage
3. **Coordinate Systems**: Minimize reprojections when possible
4. **Parallelization**: Use dlt's built-in parallel processing
5. **Streaming**: Process large datasets in chunks

## Contributing

Contributions to spatial functionality are welcome! Please see [CONTRIBUTING.md](../../../CONTRIBUTING.md).

## Resources

- **dlt Documentation**: https://dlthub.com/docs
- **GDAL/OGR**: https://gdal.org/
- **PostGIS**: https://postgis.net/
- **GeoPackage**: https://www.geopackage.org/

## License

Apache 2.0 - Free for commercial and non-commercial use

---

**Need help?** Join our [Slack community](https://dlthub.com/community) or open an issue on [GitHub](https://github.com/dlt-hub/dlt/issues).
