from typing import Any, Literal

import dlt
import time
import pytest
import os
import shutil

from playwright.sync_api import Page, expect

from tests.utils import (
    patch_home_dir,
    # autouse_test_storage,
    # preserve_environ,
    # duckdb_pipeline_location,
    # wipe_pipeline,
)

from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)

from dlt.helpers.studio import strings as app_strings


@pytest.fixture(autouse=True)
def setup_pipelines() -> Any:
    # simple pipeline
    po = dlt.pipeline(pipeline_name="one_two_three", destination="duckdb")
    po.run([1, 2, 3], table_name="one_two_three")

    # fruit pipeline
    pf = dlt.pipeline(pipeline_name="fruit_pipeline", destination="duckdb")
    pf.run(fruitshop_source())

    # never run pipeline
    dlt.pipeline(pipeline_name="never_run_pipeline", destination="duckdb")

    # no destination pipeline
    pnd = dlt.pipeline(pipeline_name="no_destination_pipeline")
    pnd.extract(fruitshop_source())


#
# helpers
#


def _go_home(page: Page) -> None:
    page.goto("http://localhost:2718")


known_sections = [
    "sync",
    "overview",
    "schema",
    "data",
    "state",
    "trace",
    "loads",
    "ibis",
]


def _open_section(
    page: Page,
    section: Literal["sync", "overview", "schema", "data", "state", "trace", "loads", "ibis"],
    close_other_sections: bool = True,
) -> None:
    if close_other_sections:
        for s in known_sections:
            if s != section:
                page.get_by_role("switch", name=s).uncheck()
    page.get_by_role("switch", name=section).check()


def test_page_loads(page: Page):
    _go_home(page)

    # check title
    expect(page).to_have_title("dlt studio")

    # check top heading
    expect(page.get_by_role("heading", name="Welcome to dltHub Studio...")).to_contain_text(
        "Welcome to dltHub Studio..."
    )

    #
    # One two three pipeline
    #

    # simple check for  one two three pipeline
    page.get_by_role("link", name="one_two_three").click()

    # sync page
    _open_section(page, "sync", close_other_sections=False)
    expect(page.get_by_text(app_strings.sync_status_success_text.split("from")[0])).to_be_visible()

    # overview page
    _open_section(page, "overview")
    expect(page.get_by_text("_storage/.dlt/pipelines/one_two_three")).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("name: one_two_three").nth(1)).to_be_attached()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.browse_data_query_result_title).nth(1)).to_be_visible()

    # check first table
    page.get_by_role("checkbox").nth(0).check()
    page.get_by_role("button", name="Run Query").click()

    # enable dlt tables
    page.get_by_role("switch", name="Show _dlt tables").check()

    # state page
    _open_section(page, "state")
    expect(
        page.get_by_text('"dataset_name": "one_two_three_dataset"')
    ).to_be_visible()  # this is part of the state yaml

    # last trace page
    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace_subtitle)).to_be_visible()
    page.get_by_text(app_strings.trace_show_raw_trace_text).click()
    expect(
        page.get_by_text('"pipeline_name": "one_two_three"').nth(0)
    ).to_be_visible()  # this is part of the trace yaml

    # loads page
    _open_section(page, "loads")
    expect(
        page.get_by_role("row", name="one_two_three").nth(0)
    ).to_be_visible()  #  this is in the loads table

    # ibis page
    _open_section(page, "ibis")
    expect(page.get_by_text(app_strings.ibis_backend_connected_text)).to_be_visible()

    #
    # Fruit pipeline
    #

    # check fruit pipeline
    _go_home(page)
    page.get_by_role("link", name="fruit_pipeline").click()

    # sync page
    _open_section(page, "sync", close_other_sections=False)
    expect(page.get_by_text(app_strings.sync_status_success_text.split("from")[0])).to_be_visible()

    # overview page
    _open_section(page, "overview")
    expect(page.get_by_text("_storage/.dlt/pipelines/fruit_pipeline")).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("name: fruitshop").nth(1)).to_be_attached()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.browse_data_query_result_title).nth(1)).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text('"dataset_name": "fruit_pipeline_dataset"')).to_be_visible()

    # last trace page
    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace_subtitle)).to_be_visible()
    page.get_by_text(app_strings.trace_show_raw_trace_text).click()
    expect(
        page.get_by_text('"pipeline_name": "fruit_pipeline"').nth(0)
    ).to_be_visible()  # this is part of the trace yaml

    # loads page
    _open_section(page, "loads")
    expect(
        page.get_by_role("row", name="fruitshop").nth(0)
    ).to_be_visible()  #  this is in the loads table

    # ibis page
    _open_section(page, "ibis")
    expect(page.get_by_text(app_strings.ibis_backend_connected_text)).to_be_visible()

    #
    # Never run pipeline
    #

    _go_home(page)
    page.get_by_role("link", name="never_run_pipeline").click()

    expect(page.get_by_text(app_strings.sync_status_error_text)).to_be_visible()
    expect(page.get_by_text("_storage/.dlt/pipelines/never_run_pipeline")).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    expect(page.get_by_text(app_strings.schema_no_default_available_text)).to_be_visible()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.browse_data_error_text)).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text('"dataset_name": "never_run_pipeline_dataset"')).to_be_visible()

    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace_subtitle)).to_be_visible()
    expect(page.get_by_text(app_strings.trace_no_trace_text.strip()).nth(0)).to_be_visible()

    # loads page
    _open_section(page, "loads")
    expect(page.get_by_text(app_strings.loads_loading_failed_text)).to_be_visible()

    _open_section(page, "ibis")
    expect(page.get_by_text(app_strings.ibis_backend_error_text)).to_be_visible()

    #
    # No destination pipeline
    #

    # check no destination pipeline
    _go_home(page)
    page.get_by_role("link", name="no_destination_pipeline").click()

    expect(page.get_by_text(app_strings.sync_status_error_text)).to_be_visible()
    expect(page.get_by_text("_storage/.dlt/pipelines/no_destination_pipeline")).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("name: fruitshop").nth(1)).to_be_attached()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.browse_data_error_text)).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text('"dataset_name": null')).to_be_visible()

    # loads page
    _open_section(page, "loads")
    expect(page.get_by_text(app_strings.loads_loading_failed_text)).to_be_visible()

    # last trace page
    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace_subtitle)).to_be_visible()
    page.get_by_text(app_strings.trace_show_raw_trace_text).click()
    expect(
        page.get_by_text('"pipeline_name": "no_destination_pipeline"').nth(0)
    ).to_be_visible()  # this is part of the trace yaml

    _open_section(page, "ibis")
    expect(page.get_by_text(app_strings.ibis_backend_error_text)).to_be_visible()
