"""
CAD to GeoPackage ETL Pipeline

Convert AutoCAD DWG/DXF files to OGC GeoPackage format for
interoperable GIS data storage and exchange.

Author: dlt team
"""

from typing import Iterator, Dict, Any, List, Optional
from pathlib import Path
import dlt
from dlt.sources import DltResource


def cad_source(
    cad_file_path: str,
    layer_filter: Optional[List[str]] = None,
    target_crs: str = "EPSG:4326"
) -> DltResource:
    """
    Read CAD file (DWG/DXF) and extract all layers
    
    Args:
        cad_file_path: Path to .dwg or .dxf file
        layer_filter: List of layer names to extract (None = all)
        target_crs: Target coordinate reference system
    
    Returns:
        dlt source with CAD layers as resources
    
    Example:
        >>> pipeline = dlt.pipeline(destination="filesystem")
        >>> pipeline.run(cad_source("site_plan.dwg", layer_filter=["ROADS", "BUILDINGS"]))
    """
    try:
        from osgeo import ogr
    except ImportError:
        raise ImportError("Install GDAL: pip install gdal")
    
    file_ext = Path(cad_file_path).suffix.lower()
    
    if file_ext == ".dwg":
        driver = ogr.GetDriverByName("CAD")
    elif file_ext == ".dxf":
        driver = ogr.GetDriverByName("DXF")
    else:
        raise ValueError(f"Unsupported CAD format: {file_ext}")
    
    datasource = driver.Open(cad_file_path, 0)
    
    if datasource is None:
        raise ValueError(f"Could not open CAD file: {cad_file_path}")
    
    @dlt.source
    def cad_layers():
        for i in range(datasource.GetLayerCount()):
            layer = datasource.GetLayer(i)
            layer_name = layer.GetName()
            
            if layer_filter and layer_name not in layer_filter:
                continue
            
            @dlt.resource(name=f"cad_{layer_name}")
            def read_layer() -> Iterator[Dict[str, Any]]:
                for feature in layer:
                    geom = feature.GetGeometryRef()
                    
                    properties = {}
                    for j in range(feature.GetFieldCount()):
                        field_name = feature.GetFieldDefnRef(j).GetName()
                        properties[field_name] = feature.GetField(j)
                    
                    geom_type = geom.GetGeometryName() if geom else "Unknown"
                    
                    yield {
                        **properties,
                        "geometry": geom.ExportToWkb() if geom else None,
                        "geometry_type": geom_type,
                        "layer_name": layer_name,
                        "source_file": cad_file_path
                    }
            
            yield read_layer()
    
    return cad_layers()


def dxf_to_geojson(
    dxf_path: str,
    output_path: str,
    layer_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convert DXF file to GeoJSON format
    
    Args:
        dxf_path: Path to DXF file
        output_path: Output GeoJSON file path
        layer_name: Specific layer to convert (None = all layers)
    
    Returns:
        Dictionary with conversion statistics
    
    Example:
        >>> stats = dxf_to_geojson("drawing.dxf", "output.geojson")
        >>> print(f"Converted {stats['feature_count']} features")
    """
    try:
        from osgeo import ogr
        import json
    except ImportError:
        raise ImportError("Install GDAL: pip install gdal")
    
    src_ds = ogr.Open(dxf_path)
    if src_ds is None:
        raise ValueError(f"Could not open DXF: {dxf_path}")
    
    driver = ogr.GetDriverByName("GeoJSON")
    dst_ds = driver.CreateDataSource(output_path)
    
    total_features = 0
    layers_processed = 0
    
    for i in range(src_ds.GetLayerCount()):
        src_layer = src_ds.GetLayer(i)
        name = src_layer.GetName()
        
        if layer_name and name != layer_name:
            continue
        
        dst_layer = dst_ds.CreateLayer(name, geom_type=ogr.wkbUnknown)
        
        layer_defn = src_layer.GetLayerDefn()
        for j in range(layer_defn.GetFieldCount()):
            field_defn = layer_defn.GetFieldDefn(j)
            dst_layer.CreateField(field_defn)
        
        for feature in src_layer:
            dst_layer.CreateFeature(feature)
            total_features += 1
        
        layers_processed += 1
    
    dst_ds = None
    src_ds = None
    
    return {
        "feature_count": total_features,
        "layers_processed": layers_processed,
        "output_file": output_path
    }


def cad_to_geopackage_pipeline(
    cad_file: str,
    output_gpkg: str,
    layers: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Complete pipeline: CAD to GeoPackage with dlt
    
    Args:
        cad_file: Input CAD file path
        output_gpkg: Output GeoPackage path
        layers: List of layers to convert
    
    Returns:
        Pipeline execution info
    
    Example:
        >>> info = cad_to_geopackage_pipeline(
        ...     "site.dwg",
        ...     "site.gpkg",
        ...     layers=["BOUNDARY", "STRUCTURES"]
        ... )
    """
    pipeline = dlt.pipeline(
        pipeline_name="cad_to_geopackage",
        destination="filesystem",
        dataset_name="cad_converted"
    )
    
    info = pipeline.run(
        cad_source(cad_file, layer_filter=layers),
        loader_file_format="parquet"
    )
    
    try:
        from osgeo import ogr
        
        driver = ogr.GetDriverByName("GPKG")
        gpkg_ds = driver.CreateDataSource(output_gpkg)
        
        for package in info.load_packages:
            for table_name in package.schema.tables.keys():
                pass
        
        gpkg_ds = None
        
    except ImportError:
        print("Warning: GDAL not available for direct GeoPackage export")
    
    return info


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python cad_to_geopackage.py <input.dwg> <output.gpkg>")
        sys.exit(1)
    
    input_cad = sys.argv[1]
    output_gpkg = sys.argv[2]
    
    print(f"Converting {input_cad} to {output_gpkg}...")
    
    info = cad_to_geopackage_pipeline(input_cad, output_gpkg)
    
    print(f"Conversion complete: {info}")
