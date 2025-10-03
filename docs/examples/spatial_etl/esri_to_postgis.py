"""
Spatial ETL Example: ESRI FileGDB to PostGIS

This example demonstrates loading spatial data from ESRI FileGDB format
into a PostGIS-enabled PostgreSQL database with transformations.

Requirements:
    pip install 'dlt[spatial,postgres]'

Features demonstrated:
    - Reading ESRI FileGDB layers
    - Coordinate reprojection
    - Geometry buffering
    - Spatial filtering
    - PostGIS destination loading
"""

import dlt
from dlt.sources.spatial import (
    read_vector,
    reproject,
    buffer_geometry,
    spatial_filter,
    validate_geometry,
)


def load_esri_to_postgis(
    filegdb_path: str = "/data/city.gdb",
    layer_name: str = "Roads",
    target_crs: str = "EPSG:4326",
    buffer_distance: float = 10.0,
):
    """
    Load ESRI FileGDB data into PostGIS with transformations

    Args:
        filegdb_path: Path to FileGDB (.gdb directory)
        layer_name: Name of layer to extract
        target_crs: Target coordinate system (default: WGS84)
        buffer_distance: Buffer distance in meters
    """

    pipeline = dlt.pipeline(
        pipeline_name="esri_to_postgis",
        destination="postgres",
        dataset_name="spatial_data",
    )

    roads = read_vector(
        file_path=filegdb_path,
        layer_name=layer_name,
        chunk_size=5000,
        geometry_format="wkt",
    )

    roads_transformed = (
        roads
        | validate_geometry(repair=True)
        | reproject(source_crs="EPSG:2154", target_crs=target_crs)
        | buffer_geometry(distance=buffer_distance)
    )

    info = pipeline.run(
        roads_transformed,
        table_name="roads_buffered",
        write_disposition="replace",
    )

    print(f"Pipeline completed: {info}")
    print(f"Loaded {info.metrics['rows']} features")

    return info


def load_multiple_layers_to_postgis(filegdb_path: str = "/data/city.gdb"):
    """
    Load multiple layers from FileGDB to PostGIS

    Args:
        filegdb_path: Path to FileGDB
    """
    from dlt.sources.spatial.helpers import list_layers

    layers_info = list_layers(filegdb_path)
    print(f"Found {len(layers_info)} layers:")
    for layer in layers_info:
        print(f"  - {layer['name']}: {layer['feature_count']} features")

    pipeline = dlt.pipeline(
        pipeline_name="multi_layer_etl",
        destination="postgres",
        dataset_name="city_gis",
    )

    for layer_info in layers_info[:5]:
        layer_name = layer_info['name']
        print(f"\nLoading layer: {layer_name}")

        features = read_vector(
            file_path=filegdb_path,
            layer_name=layer_name,
            chunk_size=1000,
        )

        features_transformed = (
            features
            | validate_geometry(repair=True, remove_invalid=True)
            | reproject(source_crs="EPSG:2154", target_crs="EPSG:4326")
        )

        pipeline.run(
            features_transformed,
            table_name=layer_name.lower(),
            write_disposition="replace",
        )

    print("\nAll layers loaded successfully!")


if __name__ == "__main__":
    load_esri_to_postgis()
