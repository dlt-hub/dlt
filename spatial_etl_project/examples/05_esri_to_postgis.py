"""
ESRI to PostGIS ETL Pipeline

Demonstrates loading ESRI formats (Shapefile, File Geodatabase, ArcGIS REST API)
into PostGIS with spatial indexing and CRS transformation.

Author: dlt team
"""

from typing import Iterator, Dict, Any, Optional, List
from pathlib import Path
import dlt
from dlt.sources import DltResource


def esri_shapefile_source(
    shapefile_path: str,
    target_crs: str = "EPSG:4326",
    batch_size: int = 1000
) -> DltResource:
    """
    Read ESRI Shapefile and prepare for PostGIS loading
    
    Args:
        shapefile_path: Path to .shp file
        target_crs: Target coordinate reference system (default: WGS84)
        batch_size: Number of features per batch
    
    Returns:
        dlt resource yielding spatial features
    
    Example:
        >>> pipeline = dlt.pipeline(destination="postgres")
        >>> pipeline.run(esri_shapefile_source("cities.shp"))
    """
    try:
        from osgeo import ogr, osr
        import json
    except ImportError:
        raise ImportError("Install GDAL: pip install gdal")
    
    @dlt.resource(name="shapefile_features")
    def read_features() -> Iterator[List[Dict[str, Any]]]:
        driver = ogr.GetDriverByName("ESRI Shapefile")
        datasource = driver.Open(shapefile_path, 0)
        
        if datasource is None:
            raise ValueError(f"Could not open shapefile: {shapefile_path}")
        
        layer = datasource.GetLayer()
        source_srs = layer.GetSpatialRef()
        
        target_srs = osr.SpatialReference()
        target_srs.ImportFromEPSG(int(target_crs.split(":")[1]))
        
        transform = None
        if source_srs and not source_srs.IsSame(target_srs):
            transform = osr.CoordinateTransformation(source_srs, target_srs)
        
        batch = []
        for feature in layer:
            geom = feature.GetGeometryRef()
            
            if geom and transform:
                geom.Transform(transform)
            
            properties = {}
            for i in range(feature.GetFieldCount()):
                field_name = feature.GetFieldDefnRef(i).GetName()
                properties[field_name] = feature.GetField(i)
            
            record = {
                **properties,
                "geometry": geom.ExportToWkb() if geom else None,
                "crs": target_crs
            }
            
            batch.append(record)
            
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        if batch:
            yield batch
        
        datasource = None
    
    return read_features()


def file_geodatabase_source(
    gdb_path: str,
    layer_names: Optional[List[str]] = None,
    target_crs: str = "EPSG:4326"
) -> DltResource:
    """
    Read ESRI File Geodatabase layers
    
    Args:
        gdb_path: Path to .gdb directory
        layer_names: List of layer names to read (None = all layers)
        target_crs: Target coordinate reference system
    
    Returns:
        dlt source with multiple resources (one per layer)
    
    Example:
        >>> pipeline.run(file_geodatabase_source("data.gdb", ["roads", "buildings"]))
    """
    try:
        from osgeo import ogr
    except ImportError:
        raise ImportError("Install GDAL: pip install gdal")
    
    driver = ogr.GetDriverByName("OpenFileGDB")
    datasource = driver.Open(gdb_path, 0)
    
    if datasource is None:
        raise ValueError(f"Could not open geodatabase: {gdb_path}")
    
    if layer_names is None:
        layer_names = [datasource.GetLayer(i).GetName() 
                      for i in range(datasource.GetLayerCount())]
    
    @dlt.source
    def gdb_layers():
        for layer_name in layer_names:
            layer = datasource.GetLayerByName(layer_name)
            if layer is None:
                continue
            
            @dlt.resource(name=layer_name)
            def read_layer() -> Iterator[Dict[str, Any]]:
                for feature in layer:
                    geom = feature.GetGeometryRef()
                    properties = {}
                    
                    for i in range(feature.GetFieldCount()):
                        field_name = feature.GetFieldDefnRef(i).GetName()
                        properties[field_name] = feature.GetField(i)
                    
                    yield {
                        **properties,
                        "geometry": geom.ExportToWkb() if geom else None,
                        "layer_name": layer_name
                    }
            
            yield read_layer()
    
    return gdb_layers()


def arcgis_rest_source(
    service_url: str,
    where_clause: str = "1=1",
    max_features: int = 1000
) -> DltResource:
    """
    Query ArcGIS REST API and load features
    
    Args:
        service_url: ArcGIS REST service URL
        where_clause: SQL WHERE clause for filtering
        max_features: Maximum features to retrieve
    
    Returns:
        dlt resource with ArcGIS features
    
    Example:
        >>> url = "https://services.arcgis.com/...../FeatureServer/0"
        >>> pipeline.run(arcgis_rest_source(url, where_clause="STATE='CA'"))
    """
    import requests
    
    @dlt.resource(name="arcgis_features")
    def query_service() -> Iterator[List[Dict[str, Any]]]:
        params = {
            "where": where_clause,
            "outFields": "*",
            "f": "geojson",
            "resultRecordCount": max_features
        }
        
        response = requests.get(f"{service_url}/query", params=params)
        response.raise_for_status()
        
        data = response.json()
        
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            geom = feature.get("geometry")
            
            from shapely.geometry import shape
            geom_obj = shape(geom) if geom else None
            
            yield {
                **props,
                "geometry": geom_obj.wkb if geom_obj else None
            }
    
    return query_service()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python esri_to_postgis.py <shapefile_path>")
        sys.exit(1)
    
    shapefile_path = sys.argv[1]
    
    pipeline = dlt.pipeline(
        pipeline_name="esri_to_postgis",
        destination="postgres",
        dataset_name="gis_data"
    )
    
    print(f"Loading {shapefile_path} to PostGIS...")
    
    info = pipeline.run(
        esri_shapefile_source(shapefile_path),
        table_name="features"
    )
    
    print(f"Load complete: {info}")
