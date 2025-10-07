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
def ogr_shapefile_pipeline() -> Any:
    try:
        from osgeo import ogr, osr
    except ImportError:
        pytest.skip("GDAL/OGR not installed")
    
    shp_file = str(TEST_DATA_DIR / "poly.shp")
    
    @dlt.resource
    def read_shapefile():
        driver = ogr.GetDriverByName("ESRI Shapefile")
        datasource = driver.Open(shp_file, 0)
        layer = datasource.GetLayer()
        
        features = []
        for feature in layer:
            geom = feature.GetGeometryRef()
            properties = {}
            
            for i in range(feature.GetFieldCount()):
                field_name = feature.GetFieldDefnRef(i).GetName()
                properties[field_name] = feature.GetField(i)
            
            properties['geometry_wkt'] = geom.ExportToWkt() if geom else None
            properties['area'] = geom.GetArea() if geom else None
            
            features.append(properties)
        
        datasource = None
        return features
    
    pipeline = dlt.pipeline(
        pipeline_name="ogr_shapefile_pipeline",
        destination="duckdb"
    )
    
    pipeline.run(
        read_shapefile(),
        table_name="polygons"
    )
    
    return pipeline


@pytest.fixture()
def ogr_countries_pipeline() -> Any:
    try:
        from osgeo import ogr, osr
    except ImportError:
        pytest.skip("GDAL/OGR not installed")
    
    shp_file = str(TEST_DATA_DIR / "ne_110m_admin_0_countries.shp")
    
    @dlt.resource
    def read_countries():
        driver = ogr.GetDriverByName("ESRI Shapefile")
        datasource = driver.Open(shp_file, 0)
        layer = datasource.GetLayer()
        
        batch = []
        for feature in layer:
            geom = feature.GetGeometryRef()
            
            record = {
                'name': feature.GetField('NAME') if feature.GetFieldIndex('NAME') >= 0 else None,
                'continent': feature.GetField('CONTINENT') if feature.GetFieldIndex('CONTINENT') >= 0 else None,
                'pop_est': feature.GetField('POP_EST') if feature.GetFieldIndex('POP_EST') >= 0 else None,
                'geometry_wkt': geom.ExportToWkt() if geom else None,
                'area': geom.GetArea() if geom else None,
                'centroid': geom.Centroid().ExportToWkt() if geom else None,
            }
            
            batch.append(record)
            
            if len(batch) >= 50:
                yield batch
                batch = []
        
        if batch:
            yield batch
        
        datasource = None
    
    pipeline = dlt.pipeline(
        pipeline_name="ogr_countries_pipeline",
        destination="duckdb"
    )
    
    pipeline.run(
        read_countries(),
        table_name="world_countries"
    )
    
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


def test_ogr_shapefile_pipeline(page: Page, ogr_shapefile_pipeline: Any):
    _go_home(page)
    page.get_by_role("link", name="ogr_shapefile_pipeline").click()
    
    _open_section(page, "overview")
    expect(page.get_by_text(_normpath("_storage/.dlt/pipelines/ogr_shapefile_pipeline"))).to_be_visible()
    
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("name: polygons").nth(0)).to_be_visible()
    
    _open_section(page, "data")
    page.get_by_role("checkbox").nth(0).check()
    page.get_by_role("button", name="Run Query").click()
    
    expect(page.get_by_text("geometry_wkt")).to_be_visible()
    expect(page.get_by_text("area")).to_be_visible()
    
    _open_section(page, "loads")
    expect(page.get_by_role("row", name="polygons").nth(0)).to_be_visible()


def test_ogr_countries_pipeline(page: Page, ogr_countries_pipeline: Any):
    _go_home(page)
    page.get_by_role("link", name="ogr_countries_pipeline").click()
    
    _open_section(page, "overview")
    expect(page.get_by_text(_normpath("_storage/.dlt/pipelines/ogr_countries_pipeline"))).to_be_visible()
    
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("world_countries")).to_be_visible()
    
    _open_section(page, "data")
    page.get_by_role("checkbox").nth(0).check()
    page.get_by_role("button", name="Run Query").click()
    
    expect(page.get_by_text("name")).to_be_visible()
    expect(page.get_by_text("continent")).to_be_visible()
    expect(page.get_by_text("area")).to_be_visible()
    
    _open_section(page, "loads")
    expect(page.get_by_role("row", name="world_countries").nth(0)).to_be_visible()
