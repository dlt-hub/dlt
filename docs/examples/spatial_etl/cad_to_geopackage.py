"""
Spatial ETL Example: CAD (DWG/DXF) to GeoPackage

This example demonstrates converting CAD files (AutoCAD DWG/DXF format)
to OGC GeoPackage format using dlt spatial capabilities.

Requirements:
    pip install 'dlt[spatial,geopackage]'

Features demonstrated:
    - Reading CAD files (DWG, DXF)
    - Attribute mapping and transformation
    - Coordinate reprojection
    - GeoPackage destination
"""

import dlt
from dlt.sources.spatial import (
    read_vector,
    reproject,
    attribute_mapper,
    validate_geometry,
    geometry_extractor,
)


def load_cad_to_geopackage(
    cad_file_path: str = "/data/building_plans.dwg",
    output_gpkg: str = "output.gpkg",
    layer_name: str = "0",
):
    """
    Convert CAD file to GeoPackage

    Args:
        cad_file_path: Path to DWG or DXF file
        output_gpkg: Output GeoPackage file path
        layer_name: CAD layer name (default: "0")
    """

    pipeline = dlt.pipeline(
        pipeline_name="cad_to_geopackage",
        destination="geopackage",
        dataset_name=output_gpkg,
    )

    cad_features = read_vector(
        file_path=cad_file_path,
        layer_name=layer_name,
        chunk_size=2000,
        geometry_format="wkt",
    )

    transformed_features = (
        cad_features
        | validate_geometry(repair=True)
        | reproject(source_crs="EPSG:3857", target_crs="EPSG:4326")
        | geometry_extractor(
            extract_centroid=True,
            extract_bounds=True,
            extract_area=True
        )
        | attribute_mapper(
            mapping={
                'Layer': 'layer_name',
                'EntityHandle': 'entity_id',
                'Color': 'color_code',
            },
            type_conversions={
                'color_code': int,
            }
        )
    )

    info = pipeline.run(
        transformed_features,
        table_name="cad_features",
        write_disposition="replace",
    )

    print(f"CAD data converted successfully!")
    print(f"Output file: {output_gpkg}")
    print(f"Features loaded: {info.metrics.get('rows', 'N/A')}")

    return info


def batch_convert_dxf_files(
    input_directory: str = "/data/cad_files",
    output_gpkg: str = "combined.gpkg",
):
    """
    Batch convert multiple DXF files into single GeoPackage

    Args:
        input_directory: Directory containing DXF files
        output_gpkg: Output GeoPackage path
    """
    from pathlib import Path

    pipeline = dlt.pipeline(
        pipeline_name="batch_cad_conversion",
        destination="geopackage",
        dataset_name=output_gpkg,
    )

    dxf_files = list(Path(input_directory).glob("*.dxf"))
    print(f"Found {len(dxf_files)} DXF files")

    for dxf_file in dxf_files:
        print(f"\nProcessing: {dxf_file.name}")

        features = read_vector(
            file_path=str(dxf_file),
            chunk_size=1000,
        )

        features_with_source = (
            features
            | attribute_mapper(
                mapping={'_filename': 'source_file'},
                drop_unmapped=False
            )
        )

        def add_source_filename(items):
            for item in items:
                item['source_file'] = dxf_file.name
                yield item

        pipeline.run(
            features_with_source,
            table_name="cad_features",
            write_disposition="append",
        )

    print(f"\nBatch conversion complete: {output_gpkg}")


if __name__ == "__main__":
    load_cad_to_geopackage()
