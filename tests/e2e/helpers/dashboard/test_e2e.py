import re
import time
from typing import Any, Literal
import asyncio
import sys
from pathlib import Path

import dlt

from dlt._workspace.helpers.dashboard.runner import start_dashboard
from dlt._workspace.run_context import switch_profile
from tests.e2e.helpers.dashboard.conftest import fruitshop_source, pipeline_path_text
from tests.workspace.utils import isolated_workspace
from tests.utils import get_test_storage_root
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
]


def _open_section(
    page: Page,
    section: Literal["overview", "schema", "data", "state", "trace", "loads"],
    close_other_sections: bool = True,
) -> None:
    if close_other_sections:
        _close_sections(page, section)
    page.get_by_role("switch", name=section).check()


def _close_sections(page: Page, skip_section: str = None) -> None:
    for s in known_sections:
        if s != skip_section:
            page.get_by_role("switch", name=s).uncheck()


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


def test_exception_pipeline(
    page: Page,
    failed_pipeline: Any,
    pipelines_dir: Path,
):
    _go_home(page)
    page.get_by_role("link", name="failed_pipeline").click()
    expect(page.get_by_text("AssertionError: I am broken").nth(0)).to_be_visible()

    # overview page
    _open_section(page, "overview")
    expect(page.get_by_text(pipeline_path_text(pipelines_dir, "failed_pipeline"))).to_be_visible()

    _open_section(page, "schema")
    expect(page.get_by_text(app_strings.schema_no_default_available_text[0:20])).to_be_visible()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.schema_no_default_available_text[0:20])).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text("_local")).to_be_visible()

    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace.subtitle)).to_be_visible()
    expect(page.get_by_text("AssertionError: I am broken").nth(0)).to_be_visible()

    # loads page
    _open_section(page, "loads")
    expect(page.get_by_text(app_strings.loads_loading_failed_text[0:20])).to_be_visible()


def test_multi_schema_selection(page: Page, multi_schema_pipeline: Any):
    _go_home(page)
    page.get_by_role("link", name="multi_schema_pipeline").click()

    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()

    expect(page.locator(".cm-line", has_text="name: fruitshop_customers").first).to_be_attached()

    def _select_schema_and_verify(
        schema_selector: Any,
        schema_name: str,
        expected: str,
        not_expected: set[str],
    ):
        schema_selector.select_option(schema_name)
        expect(schema_selector).to_have_value(schema_name)
        # allow marimo reactivity to process
        page.wait_for_timeout(500)
        expect(page.get_by_text(expected, exact=True).nth(0)).to_be_visible(timeout=15000)
        for table in not_expected:
            expect(page.get_by_text(table, exact=True)).to_have_count(0, timeout=10000)

    # select each schema and see if the right tables are shown
    # do this both for schema and data section
    for section in ["schema", "data"]:
        _open_section(page, section)  # type: ignore[arg-type]

        # NOTE: this is using unspecific selector and may select other dropdowns id present (?)
        schema_selector = page.get_by_test_id("marimo-plugin-dropdown")

        all_tables = {"customers", "inventory", "purchases"}

        _select_schema_and_verify(
            schema_selector,
            "fruitshop_customers",
            expected="customers",
            not_expected=all_tables - {"customers"},
        )
        schema_selector.scroll_into_view_if_needed()

        _select_schema_and_verify(
            schema_selector,
            "fruitshop_inventory",
            expected="inventory",
            not_expected=all_tables - {"inventory"},
        )

        _select_schema_and_verify(
            schema_selector,
            "fruitshop_purchases",
            expected="purchases",
            not_expected=all_tables - {"purchases"},
        )

        _close_sections(page)
        # make sure schema selector removed from page
        expect(schema_selector).not_to_be_attached()


def test_simple_incremental_pipeline(
    page: Page,
    simple_incremental_pipeline: Any,
    pipelines_dir: Path,
):
    #
    # One two three pipeline
    #

    # simple check for  one two three pipeline
    _go_home(page)
    page.get_by_role("link", name="one_two_three").click()

    # overview page
    _open_section(page, "overview")
    expect(page.get_by_text(pipeline_path_text(pipelines_dir, "one_two_three"))).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.locator(".cm-line", has_text="name: one_two_three").first).to_be_attached()

    # check first table and columns
    page.get_by_role("checkbox").nth(0).check()
    expect(page.get_by_role("table").get_by_text("id", exact=True)).to_be_visible()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.browse_data_query_result_title).nth(1)).to_be_visible()

    # check first table
    page.get_by_role("checkbox").nth(0).check()

    # check state (we check some info from the incremental state here)
    page.get_by_text("Show source and resource state").click()
    expect(
        page.get_by_label("Show source and resource").get_by_text(
            "unique_hashes"
        )  # unique hashes is only shown if there is incremental state
    ).to_be_visible()

    page.get_by_role("button", name="Run Query").click()

    # enable dlt tables
    page.get_by_role("switch", name="Show internal tables").check()

    # state page
    _open_section(page, "state")
    expect(page.get_by_text("_local")).to_be_visible()  # this is part of the state yaml

    # last trace page
    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace.subtitle)).to_be_visible()
    page.get_by_text(app_strings.trace_show_raw_trace_text).click()
    expect(
        page.get_by_text("execution_context").nth(0)
    ).to_be_visible()  # this is part of the trace yaml

    # loads page
    _open_section(page, "loads")
    expect(
        page.get_by_role("row", name="one_two_three").nth(0)
    ).to_be_visible()  #  this is in the loads table

    # since we are not waiting for the result but clicking ahead, pause to avoid locked duckdb
    time.sleep(2.0)


def test_fruit_pipeline(page: Page, fruit_pipeline: Any, pipelines_dir: Path):
    # check fruit pipeline
    _go_home(page)
    page.get_by_role("link", name="fruit_pipeline").click()

    # overview page
    _open_section(page, "overview")
    expect(page.get_by_text(pipeline_path_text(pipelines_dir, "fruit_pipeline"))).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.locator(".cm-line", has_text="name: fruitshop").first).to_be_attached()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.browse_data_query_result_title).nth(1)).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text("_local")).to_be_visible()

    # last trace page
    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace.subtitle)).to_be_visible()
    page.get_by_text(app_strings.trace_show_raw_trace_text).click()
    expect(
        page.get_by_text("execution_context").nth(0)
    ).to_be_visible()  # this is part of the trace yaml

    # loads page
    _open_section(page, "loads")
    expect(
        page.get_by_role("row", name="fruitshop").nth(0)
    ).to_be_visible()  #  this is in the loads table


def test_never_run_pipeline(page: Page, never_run_pipeline: Any, pipelines_dir: Path):
    _go_home(page)
    page.get_by_role("link", name="never_run_pipeline").click()

    # info closed by default
    _open_section(page, "overview")
    expect(
        page.get_by_text(pipeline_path_text(pipelines_dir, "never_run_pipeline"))
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
    expect(page.get_by_text(app_strings.trace.subtitle)).to_be_visible()
    expect(page.get_by_text(app_strings.trace_no_trace_text.strip()).nth(0)).to_be_visible()

    # loads page
    _open_section(page, "loads")
    expect(page.get_by_text(app_strings.loads_loading_failed_text[0:20])).to_be_visible()


def test_no_destination_pipeline(page: Page, no_destination_pipeline: Any, pipelines_dir: Path):
    # check no destination pipeline
    _go_home(page)
    page.get_by_role("link", name="no_destination_pipeline").click()

    # info closed by default
    _open_section(page, "overview")
    expect(
        page.get_by_text(pipeline_path_text(pipelines_dir, "no_destination_pipeline"))
    ).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.locator(".cm-line", has_text="name: fruitshop").first).to_be_attached()

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
    expect(page.get_by_text(app_strings.trace.subtitle)).to_be_visible()
    page.get_by_text(app_strings.trace_show_raw_trace_text).click()
    expect(
        page.get_by_text("execution_context").nth(0)
    ).to_be_visible()  # this is only shown in trace yaml


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

            expect(page.get_by_role("switch", name="overview")).to_be_visible(timeout=20000)
            page.get_by_role("switch", name="loads").check()
            expect(page.get_by_role("row", name="fruitshop").first).to_be_visible()


def test_broken_trace_pipeline(page: Page, broken_trace_pipeline: Any, pipelines_dir: Path):
    """Dashboard should still render overview even if the last trace file is corrupted."""
    _go_home(page)
    page.get_by_role("link", name="broken_trace_pipeline").click()

    # overview page should still be accessible and show the working dir path
    _open_section(page, "overview")
    expect(
        page.get_by_text(pipeline_path_text(pipelines_dir, "broken_trace_pipeline"))
    ).to_be_visible()

    # should also render the trace section, but there should be an error message
    _open_section(page, "trace")
    expect(page.get_by_text("Error while building trace section:")).to_be_visible()


def test_sections_query_param(page: Page, fruit_pipeline: Any):
    """Sections specified in ?sections= query param should be pre-opened."""
    # navigate with sections=trace,loads in the URL
    page.goto("http://localhost:2718/?pipeline=fruit_pipeline&sections=trace,loads")

    # wait for the pipeline to load
    expect(page.get_by_role("switch", name="trace")).to_be_visible(timeout=20000)

    # trace and loads switches should be checked
    expect(page.get_by_role("switch", name="trace")).to_be_checked()
    expect(page.get_by_role("switch", name="loads")).to_be_checked()

    # other sections should NOT be checked
    expect(page.get_by_role("switch", name="overview")).not_to_be_checked()
    expect(page.get_by_role("switch", name="schema")).not_to_be_checked()
    expect(page.get_by_role("switch", name="data")).not_to_be_checked()
    expect(page.get_by_role("switch", name="state")).not_to_be_checked()

    # verify the trace section content is actually visible
    expect(page.get_by_text("An overview of the last load trace")).to_be_visible(timeout=15000)

    # verify loads section content is visible
    expect(page.get_by_role("row", name="fruitshop").nth(0)).to_be_visible(timeout=15000)


def test_sections_query_param_all(page: Page, fruit_pipeline: Any):
    """All sections should open when all are specified in ?sections= query param."""
    page.goto(
        "http://localhost:2718/?pipeline=fruit_pipeline"
        "&sections=overview,schema,data,state,trace,loads"
    )

    # wait for the pipeline to load
    expect(page.get_by_role("switch", name="overview")).to_be_visible(timeout=20000)

    # all specified switches should be checked
    for section in ["overview", "schema", "data", "state", "trace", "loads"]:
        expect(page.get_by_role("switch", name=section)).to_be_checked()
