import re
import time
from typing import Any, Literal
import asyncio
import sys
import pathlib

import dlt

from dlt._workspace.helpers.dashboard.runner import start_dashboard
from dlt._workspace.run_context import switch_profile
from tests.e2e.helpers.dashboard.conftest import fruitshop_source, _normpath
from tests.workspace.utils import isolated_workspace
from playwright.sync_api import Page, expect

from tests.utils import (
    auto_test_run_context,
    autouse_test_storage,
    preserve_environ,
    deactivate_pipeline,
)
from dlt._workspace.helpers.dashboard import strings as app_strings, utils

# NOTE: The line below is needed for playwright to work on windows
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

#
# helpers
#


def _go_home(page: Page) -> None:
    page.goto("http://localhost:2718")


known_sections = [
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
    section: Literal["overview", "schema", "data", "state", "trace", "loads", "ibis"],
    close_other_sections: bool = True,
) -> None:
    if close_other_sections:
        for s in known_sections:
            if s != section:
                page.get_by_role("switch", name=s).uncheck()
    page.get_by_role("switch", name=section).check()


def test_page_overview(page: Page):
    _go_home(page)

    # check title
    expect(page).to_have_title("dlt workspace dashboard")

    # check top heading
    expect(
        page.get_by_role("heading", name="Welcome to the dltHub workspace dashboard...")
    ).to_contain_text(
        "Welcome to the dltHub workspace dashboard..."
    )  #

    #
    # Exception pipeline
    #


def test_exception_pipeline(page: Page, failed_pipeline: Any):
    _go_home(page)
    page.get_by_role("link", name="failed_pipeline").click()
    expect(page.get_by_text("AssertionError: I am broken").nth(0)).to_be_visible()

    # overview page
    _open_section(page, "overview")
    expect(page.get_by_text(_normpath("_storage/.dlt/pipelines/failed_pipeline"))).to_be_visible()

    _open_section(page, "schema")
    expect(page.get_by_text(app_strings.schema_no_default_available_text[0:20])).to_be_visible()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.schema_no_default_available_text[0:20])).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text("_local")).to_be_visible()

    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace_subtitle)).to_be_visible()
    expect(page.get_by_text("AssertionError: I am broken").nth(0)).to_be_visible()

    # loads page
    _open_section(page, "loads")
    expect(page.get_by_text(app_strings.loads_loading_failed_text[0:20])).to_be_visible()

    # _open_section(page, "ibis")
    # expect(page.get_by_text(app_strings.ibis_backend_error_text[0:20])).to_be_visible()


def test_multi_schema_selection(page: Page, multi_schema_pipeline: Any):
    _go_home(page)
    page.get_by_role("link", name="multi_schema_pipeline").click()

    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()

    expect(page.get_by_text("name: fruitshop_customers").nth(1)).to_be_attached()

    # select each schema and see if the right tables are shown
    # do this both for schema and data section
    for section in ["schema", "data"]:
        _open_section(page, section)  # type: ignore[arg-type]

        schema_selector = page.get_by_test_id("marimo-plugin-dropdown")
        schema_selector.select_option("fruitshop_customers")
        expect(page.get_by_text("customers", exact=True).nth(0)).to_be_visible()
        expect(page.get_by_text("inventory", exact=True)).to_have_count(0)
        expect(page.get_by_text("purchases", exact=True)).to_have_count(0)

        schema_selector.select_option("fruitshop_inventory")
        expect(page.get_by_text("inventory", exact=True).nth(0)).to_be_visible()
        expect(page.get_by_text("customers", exact=True)).to_have_count(0)
        expect(page.get_by_text("purchases", exact=True)).to_have_count(0)

        schema_selector.select_option("fruitshop_purchases")
        expect(page.get_by_text("purchases", exact=True).nth(0)).to_be_visible()
        expect(page.get_by_text("inventory", exact=True)).to_have_count(0)
        expect(page.get_by_text("customers", exact=True)).to_have_count(0)


def test_simple_incremental_pipeline(page: Page, simple_incremental_pipeline: Any):
    #
    # One two three pipeline
    #

    # simple check for  one two three pipeline
    _go_home(page)
    page.get_by_role("link", name="one_two_three").click()

    # overview page
    _open_section(page, "overview")
    expect(page.get_by_text(_normpath("_storage/.dlt/pipelines/one_two_three"))).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("name: one_two_three").nth(1)).to_be_attached()

    # check first table and columns
    page.get_by_role("checkbox").nth(0).check()
    expect(page.get_by_text("id", exact=True)).to_be_visible()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.browse_data_query_result_title).nth(1)).to_be_visible()

    # check first table
    page.get_by_role("checkbox").nth(0).check()

    # since we are not waiting for the result but clicking ahead, pause to avoid locked duckdb
    time.sleep(2.0)

    # check state (we check some info from the incremental state here)
    page.get_by_text("Show source and resource state").click()
    expect(
        page.get_by_label("Show source and resource").get_by_text(
            "unique_hashes"
        )  # unique hashes is only shown if there is incremental state
    ).to_be_visible()

    page.get_by_role("button", name="Run Query").click()

    # enable dlt tables
    page.get_by_role("switch", name="Show _dlt tables").check()

    # state page
    _open_section(page, "state")
    expect(page.get_by_text("_local")).to_be_visible()  # this is part of the state yaml

    # last trace page
    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace_subtitle)).to_be_visible()
    page.get_by_text(app_strings.trace_show_raw_trace_text).click()
    expect(
        page.get_by_text("execution_context").nth(0)
    ).to_be_visible()  # this is part of the trace yaml

    # loads page
    _open_section(page, "loads")
    expect(
        page.get_by_role("row", name="one_two_three").nth(0)
    ).to_be_visible()  #  this is in the loads table

    # ibis page
    # _open_section(page, "ibis")
    # expect(page.get_by_text(app_strings.ibis_backend_connected_text)).to_be_visible()


def test_fruit_pipeline(page: Page, fruit_pipeline: Any):
    # check fruit pipeline
    _go_home(page)
    page.get_by_role("link", name="fruit_pipeline").click()

    # overview page
    _open_section(page, "overview")
    expect(page.get_by_text(_normpath("_storage/.dlt/pipelines/fruit_pipeline"))).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("name: fruitshop").nth(1)).to_be_attached()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.browse_data_query_result_title).nth(1)).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text("_local")).to_be_visible()

    # last trace page
    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace_subtitle)).to_be_visible()
    page.get_by_text(app_strings.trace_show_raw_trace_text).click()
    expect(
        page.get_by_text("execution_context").nth(0)
    ).to_be_visible()  # this is part of the trace yaml

    # loads page
    _open_section(page, "loads")
    expect(
        page.get_by_role("row", name="fruitshop").nth(0)
    ).to_be_visible()  #  this is in the loads table

    # ibis page
    # _open_section(page, "ibis")
    # expect(page.get_by_text(app_strings.ibis_backend_connected_text)).to_be_visible()


def test_never_run_pipeline(page: Page, never_run_pipeline: Any):
    _go_home(page)
    page.get_by_role("link", name="never_run_pipeline").click()

    expect(
        page.get_by_text(_normpath("_storage/.dlt/pipelines/never_run_pipeline"))
    ).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    expect(page.get_by_text(app_strings.schema_no_default_available_text[0:20])).to_be_visible()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.schema_no_default_available_text[0:20])).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text("_local")).to_be_visible()

    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace_subtitle)).to_be_visible()
    expect(page.get_by_text(app_strings.trace_no_trace_text.strip()).nth(0)).to_be_visible()

    # loads page
    _open_section(page, "loads")
    expect(page.get_by_text(app_strings.loads_loading_failed_text[0:20])).to_be_visible()

    # _open_section(page, "ibis")
    # expect(page.get_by_text(app_strings.ibis_backend_error_text[0:20])).to_be_visible()


def test_no_destination_pipeline(page: Page, no_destination_pipeline: Any):
    # check no destination pipeline
    _go_home(page)
    page.get_by_role("link", name="no_destination_pipeline").click()

    expect(
        page.get_by_text(_normpath("_storage/.dlt/pipelines/no_destination_pipeline"))
    ).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("name: fruitshop").nth(1)).to_be_attached()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.browse_data_error_text[0:20])).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text("_local")).to_be_visible()

    # loads page
    _open_section(page, "loads")
    expect(page.get_by_text(app_strings.loads_loading_failed_text[0:20])).to_be_visible()

    # last trace page
    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace_subtitle)).to_be_visible()
    page.get_by_text(app_strings.trace_show_raw_trace_text).click()
    expect(
        page.get_by_text("execution_context").nth(0)
    ).to_be_visible()  # this is only shown in trace yaml

    # _open_section(page, "ibis")
    # expect(page.get_by_text(app_strings.ibis_backend_error_text[0:20])).to_be_visible()


def test_workspace_profile_prod(page: Page):
    test_port = 2719
    with isolated_workspace("pipelines"):
        switch_profile("prod")
        pf = dlt.pipeline(pipeline_name="fruit_pipeline", destination="duckdb")
        pf.run(fruitshop_source())

        with start_dashboard(port=test_port):
            page.goto(f"http://localhost:{test_port}/?profile=tests")
            expect(page).to_have_url(re.compile(rf":{test_port}/\?profile=tests$"))
            expect(page.get_by_role("row", name="fruitshop").first).not_to_be_visible()

            page.goto(f"http://localhost:{test_port}/?profile=prod&pipeline=fruit_pipeline")
            expect(page.get_by_role("switch", name="overview")).to_be_visible(timeout=20000)
            page.get_by_role("switch", name="loads").check()
            expect(page.get_by_role("row", name="fruitshop").first).to_be_visible()


def test_workspace_profile_dev(page: Page):
    # NOTE: we must use different port otherwise some leftovers from previous session (cookies?)
    # persist in chromium which fails.
    test_port = 2720
    with isolated_workspace("default"):
        switch_profile("dev")
        pf = dlt.pipeline(pipeline_name="fruit_pipeline", destination="duckdb")
        pf.run(fruitshop_source())

        with start_dashboard(port=test_port):
            page.goto(f"http://localhost:{test_port}/?profile=prod")
            expect(page).to_have_url(re.compile(rf":{test_port}/\?profile=prod$"))
            expect(page.get_by_role("row", name="fruitshop").first).not_to_be_visible()

            page.goto(f"http://localhost:{test_port}/?profile=dev&pipeline=fruit_pipeline")

            expect(page.get_by_role("switch", name="overview")).to_be_visible()
            page.get_by_role("switch", name="loads").check()
            expect(page.get_by_role("row", name="fruitshop").first).to_be_visible()
