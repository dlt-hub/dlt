"""
Spatial ETL Example: Raster Data Processing

This example demonstrates processing raster data (GeoTIFF, NetCDF, etc.)
using dlt spatial capabilities with GDAL.

Requirements:
    pip install 'dlt[spatial,postgres]'

Features demonstrated:
    - Reading GeoTIFF raster data
    - Raster reprojection
    - Chunked raster processing
    - Metadata extraction
"""

import dlt
from dlt.sources.spatial import read_raster


def load_elevation_data_to_postgres(
    raster_path: str = "/data/elevation/dem.tif",
    target_crs: str = "EPSG:4326",
):
    """
    Load raster elevation data with metadata extraction

    Args:
        raster_path: Path to GeoTIFF file
        target_crs: Target coordinate system
    """

    pipeline = dlt.pipeline(
        pipeline_name="raster_etl",
        destination="postgres",
        dataset_name="elevation_data",
    )

    elevation_data = read_raster(
        file_path=raster_path,
        bands=[1],
        target_crs=target_crs,
        resample_method="cubic",
        output_format="bytes",
        chunk_size=(512, 512),
    )

    info = pipeline.run(
        elevation_data,
        table_name="elevation_tiles",
        write_disposition="replace",
    )

    print(f"Raster data loaded: {info}")
    return info


def process_sentinel_imagery(
    imagery_path: str = "/data/sentinel/image.tif",
):
    """
    Process multi-band satellite imagery (Sentinel, Landsat)

    Args:
        imagery_path: Path to multi-band raster
    """

    pipeline = dlt.pipeline(
        pipeline_name="satellite_imagery",
        destination="postgres",
        dataset_name="remote_sensing",
    )

    bands = [1, 2, 3, 4]

    imagery_data = read_raster(
        file_path=imagery_path,
        bands=bands,
        output_format="base64",
        chunk_size=(256, 256),
    )

    info = pipeline.run(
        imagery_data,
        table_name="satellite_tiles",
        write_disposition="replace",
    )

    print(f"Imagery processed: {len(bands)} bands")
    return info


def extract_raster_metadata(raster_path: str):
    """
    Extract and display raster metadata

    Args:
        raster_path: Path to raster file
    """
    from osgeo import gdal

    dataset = gdal.Open(raster_path)

    metadata = {
        'width': dataset.RasterXSize,
        'height': dataset.RasterYSize,
        'bands': dataset.RasterCount,
        'projection': dataset.GetProjection(),
        'geotransform': dataset.GetGeoTransform(),
        'driver': dataset.GetDriver().ShortName,
    }

    print("Raster Metadata:")
    for key, value in metadata.items():
        print(f"  {key}: {value}")

    for band_idx in range(1, dataset.RasterCount + 1):
        band = dataset.GetRasterBand(band_idx)
        print(f"\nBand {band_idx}:")
        print(f"  Data type: {gdal.GetDataTypeName(band.DataType)}")
        print(f"  NoData value: {band.GetNoDataValue()}")

        try:
            stats = band.GetStatistics(True, True)
            print(f"  Min: {stats[0]}, Max: {stats[1]}")
            print(f"  Mean: {stats[2]}, StdDev: {stats[3]}")
        except Exception as e:
            print(f"  Statistics unavailable: {e}")

    dataset = None


if __name__ == "__main__":
    extract_raster_metadata("/path/to/raster.tif")
