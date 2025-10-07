from typing import Any
import os
import sys
import asyncio
import pytest
from pathlib import Path

import dlt

from playwright.sync_api import Page, expect

from tests.utils import (
    patch_home_dir,
    autouse_test_storage,
    preserve_environ,
    wipe_pipeline,
)

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


TEST_DATA_DIR = Path(__file__).parent / "data" / "spatial"


@pytest.fixture()
def sedona_spatial_pipeline() -> Any:
    try:
        from sedona.spark import SedonaContext
        from pyspark.sql import SparkSession
        from dlt.sources.sedona import read_sedona_table
    except ImportError:
        pytest.skip("Sedona not installed")
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("dlt_sedona_test") \
        .config("spark.jars.packages", "org.apache.sedona:sedona-spark-3.5_2.12:1.8.0,org.datasyslab:geotools-wrapper:1.8.0-28.5") \
        .config("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
        .getOrCreate()
    
    sedona = SedonaContext.create(spark)
    
    shp_file = str(TEST_DATA_DIR / "ne_110m_admin_0_countries.shp")
    
    df = sedona.read.format("shapefile").load(shp_file)
    
    df_with_area = df.selectExpr(
        "*",
        "ST_Area(geometry) as area",
        "ST_Length(geometry) as perimeter"
    )
    
    pipeline = dlt.pipeline(
        pipeline_name="sedona_spatial_pipeline",
        destination="duckdb"
    )
    
    pipeline.run(
        read_sedona_table(df_with_area, batch_size=50),
        table_name="world_countries"
    )
    
    spark.stop()
    return pipeline


@pytest.fixture()
def sedona_spatial_join_pipeline() -> Any:
    try:
        from sedona.spark import SedonaContext
        from pyspark.sql import SparkSession
        from dlt.sources.sedona import read_sedona_sql
    except ImportError:
        pytest.skip("Sedona not installed")
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("dlt_sedona_join_test") \
        .config("spark.jars.packages", "org.apache.sedona:sedona-spark-3.5_2.12:1.8.0,org.datasyslab:geotools-wrapper:1.8.0-28.5") \
        .config("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
        .getOrCreate()
    
    sedona = SedonaContext.create(spark)
    
    shp_file = str(TEST_DATA_DIR / "ne_110m_admin_0_countries.shp")
    
    df = sedona.read.format("shapefile").load(shp_file)
    df.createOrReplaceTempView("countries")
    
    spatial_query = """
        SELECT 
            NAME as country_name,
            CONTINENT,
            ST_AsText(geometry) as geometry_wkt,
            ST_Area(geometry) as area,
            ST_Centroid(geometry) as centroid,
            CASE 
                WHEN ST_Area(geometry) > 1000000 THEN 'Large'
                WHEN ST_Area(geometry) > 100000 THEN 'Medium'
                ELSE 'Small'
            END as size_category
        FROM countries
        WHERE ST_IsValid(geometry) = true
        ORDER BY area DESC
        LIMIT 50
    """
    
    pipeline = dlt.pipeline(
        pipeline_name="sedona_spatial_join_pipeline",
        destination="duckdb"
    )
    
    pipeline.run(
        read_sedona_sql(spatial_query, sedona, batch_size=25),
        table_name="country_analysis"
    )
    
    spark.stop()
    return pipeline


def _normpath(path: str) -> str:
    import sys
    import pathlib
    return str(pathlib.Path(path)) if sys.platform.startswith("win") else path


def _go_home(page: Page) -> None:
    page.goto("http://localhost:2718")


def _open_section(page: Page, section: str) -> None:
    known_sections = ["overview", "schema", "data", "state", "trace", "loads", "ibis"]
    for s in known_sections:
        if s != section:
            page.get_by_role("switch", name=s).uncheck()
    page.get_by_role("switch", name=section).check()


def test_sedona_spatial_pipeline(page: Page, sedona_spatial_pipeline: Any):
    _go_home(page)
    page.get_by_role("link", name="sedona_spatial_pipeline").click()
    
    _open_section(page, "overview")
    expect(page.get_by_text(_normpath("_storage/.dlt/pipelines/sedona_spatial_pipeline"))).to_be_visible()
    
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("name: world_countries").nth(0)).to_be_visible()
    
    _open_section(page, "data")
    page.get_by_role("checkbox").nth(0).check()
    page.get_by_role("button", name="Run Query").click()
    
    expect(page.get_by_text("geometry")).to_be_visible()
    expect(page.get_by_text("area")).to_be_visible()
    
    _open_section(page, "loads")
    expect(page.get_by_role("row", name="world_countries").nth(0)).to_be_visible()


def test_sedona_spatial_join_pipeline(page: Page, sedona_spatial_join_pipeline: Any):
    _go_home(page)
    page.get_by_role("link", name="sedona_spatial_join_pipeline").click()
    
    _open_section(page, "overview")
    expect(page.get_by_text(_normpath("_storage/.dlt/pipelines/sedona_spatial_join_pipeline"))).to_be_visible()
    
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("country_analysis")).to_be_visible()
    
    _open_section(page, "data")
    page.get_by_role("checkbox").nth(0).check()
    page.get_by_role("button", name="Run Query").click()
    
    expect(page.get_by_text("country_name")).to_be_visible()
    expect(page.get_by_text("area")).to_be_visible()
    expect(page.get_by_text("size_category")).to_be_visible()
    
    _open_section(page, "loads")
    expect(page.get_by_role("row", name="country_analysis").nth(0)).to_be_visible()
