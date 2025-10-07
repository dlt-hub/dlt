"""
Raster Processing ETL Pipeline

Process satellite imagery, DEMs, and raster datasets for analytics.
Extract metadata, statistics, and prepare for visualization.

Author: dlt team
"""

from typing import Iterator, Dict, Any, List, Optional, Tuple
from pathlib import Path
import dlt
from dlt.sources import DltResource


def raster_metadata_source(
    raster_pattern: str,
    extract_stats: bool = True,
    compute_histogram: bool = False
) -> DltResource:
    """
    Extract metadata from raster files (GeoTIFF, COG, etc.)
    
    Args:
        raster_pattern: Glob pattern for raster files (e.g., "*.tif")
        extract_stats: Calculate band statistics
        compute_histogram: Generate histogram data
    
    Returns:
        dlt resource with raster metadata
    
    Example:
        >>> pipeline = dlt.pipeline(destination="duckdb")
        >>> pipeline.run(raster_metadata_source("/imagery/*.tif"))
    """
    try:
        from osgeo import gdal
        import numpy as np
    except ImportError:
        raise ImportError("Install GDAL and NumPy: pip install gdal numpy")
    
    from glob import glob
    
    @dlt.resource(name="raster_metadata")
    def extract_metadata() -> Iterator[Dict[str, Any]]:
        for raster_path in glob(raster_pattern):
            dataset = gdal.Open(raster_path)
            
            if dataset is None:
                continue
            
            geotransform = dataset.GetGeoTransform()
            projection = dataset.GetProjection()
            
            metadata = {
                "file_path": raster_path,
                "file_name": Path(raster_path).name,
                "width": dataset.RasterXSize,
                "height": dataset.RasterYSize,
                "band_count": dataset.RasterCount,
                "projection": projection,
                "geotransform": list(geotransform) if geotransform else None,
                "pixel_size_x": geotransform[1] if geotransform else None,
                "pixel_size_y": abs(geotransform[5]) if geotransform else None,
            }
            
            bounds = calculate_bounds(dataset)
            metadata["bounds"] = bounds
            
            bands = []
            for i in range(1, dataset.RasterCount + 1):
                band = dataset.GetRasterBand(i)
                
                band_info = {
                    "band_number": i,
                    "data_type": gdal.GetDataTypeName(band.DataType),
                    "no_data_value": band.GetNoDataValue(),
                }
                
                if extract_stats:
                    stats = band.GetStatistics(True, True)
                    band_info.update({
                        "min": stats[0],
                        "max": stats[1],
                        "mean": stats[2],
                        "std_dev": stats[3]
                    })
                
                if compute_histogram:
                    hist = band.GetHistogram()
                    band_info["histogram"] = hist
                
                bands.append(band_info)
            
            metadata["bands"] = bands
            
            dataset = None
            
            yield metadata
    
    return extract_metadata()


def calculate_bounds(dataset) -> Dict[str, float]:
    """Calculate geographic bounds of raster"""
    geotransform = dataset.GetGeoTransform()
    
    if geotransform is None:
        return {}
    
    width = dataset.RasterXSize
    height = dataset.RasterYSize
    
    min_x = geotransform[0]
    max_y = geotransform[3]
    max_x = min_x + width * geotransform[1]
    min_y = max_y + height * geotransform[5]
    
    return {
        "min_x": min_x,
        "max_x": max_x,
        "min_y": min_y,
        "max_y": max_y
    }


def raster_tile_generator(
    raster_path: str,
    tile_size: int = 256,
    output_dir: str = "./tiles"
) -> DltResource:
    """
    Generate web map tiles from raster
    
    Args:
        raster_path: Path to raster file
        tile_size: Tile size in pixels (default: 256)
        output_dir: Output directory for tiles
    
    Returns:
        dlt resource with tile metadata
    
    Example:
        >>> pipeline.run(raster_tile_generator("dem.tif", tile_size=512))
    """
    try:
        from osgeo import gdal
        import numpy as np
    except ImportError:
        raise ImportError("Install GDAL and NumPy")
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    @dlt.resource(name="raster_tiles")
    def generate_tiles() -> Iterator[Dict[str, Any]]:
        dataset = gdal.Open(raster_path)
        
        if dataset is None:
            return
        
        width = dataset.RasterXSize
        height = dataset.RasterYSize
        
        for y in range(0, height, tile_size):
            for x in range(0, width, tile_size):
                tile_width = min(tile_size, width - x)
                tile_height = min(tile_size, height - y)
                
                tile_path = f"{output_dir}/tile_{x}_{y}.tif"
                
                gdal.Translate(
                    tile_path,
                    dataset,
                    srcWin=[x, y, tile_width, tile_height]
                )
                
                yield {
                    "tile_path": tile_path,
                    "x_offset": x,
                    "y_offset": y,
                    "width": tile_width,
                    "height": tile_height,
                    "source_raster": raster_path
                }
        
        dataset = None
    
    return generate_tiles()


def zonal_statistics(
    raster_path: str,
    zones_shapefile: str,
    statistics: List[str] = ["mean", "min", "max", "sum"]
) -> DltResource:
    """
    Calculate zonal statistics for raster within polygon zones
    
    Args:
        raster_path: Path to raster file
        zones_shapefile: Path to zones shapefile
        statistics: List of statistics to calculate
    
    Returns:
        dlt resource with zonal statistics
    
    Example:
        >>> pipeline.run(zonal_statistics("temperature.tif", "counties.shp"))
    """
    try:
        from osgeo import gdal, ogr
        import numpy as np
        from rasterstats import zonal_stats as rs_zonal_stats
    except ImportError:
        raise ImportError("Install: pip install gdal rasterstats")
    
    @dlt.resource(name="zonal_statistics")
    def calculate_stats() -> Iterator[Dict[str, Any]]:
        stats_results = rs_zonal_stats(
            zones_shapefile,
            raster_path,
            stats=statistics,
            geojson_out=True
        )
        
        for i, result in enumerate(stats_results):
            zone_stats = {
                "zone_id": i,
                **result.get("properties", {}),
                "geometry": result.get("geometry")
            }
            
            yield zone_stats
    
    return calculate_stats()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python raster_processing.py <raster_pattern>")
        print("Example: python raster_processing.py '/data/imagery/*.tif'")
        sys.exit(1)
    
    raster_pattern = sys.argv[1]
    
    pipeline = dlt.pipeline(
        pipeline_name="raster_processing",
        destination="duckdb",
        dataset_name="earth_observation"
    )
    
    print(f"Processing rasters: {raster_pattern}")
    
    info = pipeline.run(
        raster_metadata_source(raster_pattern, extract_stats=True),
        table_name="raster_catalog"
    )
    
    print(f"Processing complete: {info}")
