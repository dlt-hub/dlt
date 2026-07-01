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
    auto_module_test_storage,
    auto_module_test_run_context,
    preserve_module_environ,
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


def _open_pipeline(page: Page, pipeline_name: str) -> None:
    page.goto(f"http://localhost:2718/?pipeline={pipeline_name}")


known_sections = [
    "overview",
    "schema",
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
    # the data section is always shown and has no toggle
    if section != "data":
        page.get_by_role("switch", name=section).check()


def _close_sections(page: Page, skip_section: str = None) -> None:
    for s in known_sections:
        if s != skip_section:
            page.get_by_role("switch", name=s).uncheck()


def _pipeline_selector(page: Page) -> Any:
    """Locator for the pipeline multiselect trigger (a popover button in shadow DOM)."""
    return page.locator("marimo-multiselect").locator('[aria-haspopup="dialog"]')


def _toggle_pipeline(page: Page, pipeline_name: str) -> None:
    """Open the pipeline multiselect and toggle the given option.

    With `max_selections=1` this selects the pipeline when it is not the current
    one and deselects it when it is, so it covers both selecting and clearing.
    """
    _pipeline_selector(page).click()
    page.get_by_role("option", name=pipeline_name, exact=True).click()
    page.keyboard.press("Escape")


def test_page_overview(page: Page, fruit_pipeline: Any):
    _open_pipeline(page, "fruit_pipeline")

    expect(page).to_have_title("dlt workspace dashboard")
    expect(page.get_by_text(app_strings.browse_data_query_result_title).nth(1)).to_be_visible(
        timeout=20000
    )

    #
    # Exception pipeline
    #


def test_exception_pipeline(
    page: Page,
    failed_pipeline: Any,
    pipelines_dir: Path,
):
    _open_pipeline(page, "failed_pipeline")
    expect(page.get_by_text("AssertionError: I am broken").nth(0)).to_be_visible()

    # overview page
    _open_section(page, "overview")
    expect(page.get_by_text(pipeline_path_text(pipelines_dir, "failed_pipeline"))).to_be_visible()

    _open_section(page, "schema")
    expect(
        page.get_by_text(app_strings.schema_no_default_available_text[0:20]).first
    ).to_be_visible()

    # browse data
    _open_section(page, "data")
    expect(
        page.get_by_text(app_strings.schema_no_default_available_text[0:20]).first
    ).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text("_local")).to_be_visible()

    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace.subtitle)).to_be_visible()
    expect(page.get_by_text("AssertionError: I am broken").nth(0)).to_be_visible()

    # loads page
    _open_section(page, "loads")
    expect(page.get_by_text(app_strings.loads_loading_failed_text[0:20])).to_be_visible()


def test_multi_schema_selection(page: Page, multi_schema_pipeline: Any):
    _open_pipeline(page, "multi_schema_pipeline")

    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.locator(".cm-line", has_text="name: fruitshop_customers").first).to_be_attached(
        timeout=15000
    )

    # close the schema section so only the data section's schema selector is present (same widget)
    _close_sections(page)

    schemas = ["fruitshop_customers", "fruitshop_inventory", "fruitshop_purchases"]
    all_tables = {"customers", "inventory", "purchases"}

    def _select_schema_and_verify(
        schema_name: str,
        expected: str,
        not_expected: set[str],
    ):
        schema_selector = page.get_by_test_id("marimo-plugin-dropdown").first
        expected_row = page.get_by_role("row", name=expected).first
        # marimo can drop a rapid reactive update under load; re-fire until the table updates
        for _attempt in range(3):
            schema_selector.select_option(schema_name)
            expect(schema_selector).to_have_value(schema_name)
            try:
                expect(expected_row).to_be_visible(timeout=10000)
                break
            except AssertionError:
                schema_selector.select_option(next(s for s in schemas if s != schema_name))
                page.wait_for_timeout(300)
        else:
            expect(expected_row).to_be_visible(timeout=10000)
        # assert on rows; the SQL editor and dropdown options also contain table names
        for table in not_expected:
            expect(page.get_by_role("row", name=table)).to_have_count(0, timeout=10000)

    _select_schema_and_verify(
        "fruitshop_customers",
        expected="customers",
        not_expected=all_tables - {"customers"},
    )
    _select_schema_and_verify(
        "fruitshop_inventory",
        expected="inventory",
        not_expected=all_tables - {"inventory"},
    )
    _select_schema_and_verify(
        "fruitshop_purchases",
        expected="purchases",
        not_expected=all_tables - {"purchases"},
    )


def test_simple_incremental_pipeline(
    page: Page,
    simple_incremental_pipeline: Any,
    pipelines_dir: Path,
):
    #
    # One two three pipeline
    #

    # simple check for  one two three pipeline
    _open_pipeline(page, "one_two_three")

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
    page.get_by_text(app_strings.trace_show_raw_trace_text, exact=True).click()
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
    _open_pipeline(page, "fruit_pipeline")

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
    page.get_by_text(app_strings.trace_show_raw_trace_text, exact=True).click()
    expect(
        page.get_by_text("execution_context").nth(0)
    ).to_be_visible()  # this is part of the trace yaml

    # loads page
    _open_section(page, "loads")
    expect(
        page.get_by_role("row", name="fruitshop").nth(0)
    ).to_be_visible()  #  this is in the loads table


def test_never_run_pipeline(page: Page, never_run_pipeline: Any, pipelines_dir: Path):
    _open_pipeline(page, "never_run_pipeline")

    # info closed by default
    _open_section(page, "overview")
    expect(
        page.get_by_text(pipeline_path_text(pipelines_dir, "never_run_pipeline"))
    ).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    expect(
        page.get_by_text(app_strings.schema_no_default_available_text[0:20]).first
    ).to_be_visible()

    # browse data
    _open_section(page, "data")
    expect(
        page.get_by_text(app_strings.schema_no_default_available_text[0:20]).first
    ).to_be_visible()

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
    _open_pipeline(page, "no_destination_pipeline")

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
    expect(page.get_by_text(app_strings.browse_data_error_text[0:20]).first).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text("_local")).to_be_visible()

    # loads page
    _open_section(page, "loads")
    expect(page.get_by_text(app_strings.loads_loading_failed_text[0:20])).to_be_visible()

    # last trace page
    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace.subtitle)).to_be_visible()
    page.get_by_text(app_strings.trace_show_raw_trace_text, exact=True).click()
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
    _open_pipeline(page, "broken_trace_pipeline")

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

    # trace and loads switches should be checked (verifies URL params are parsed correctly)
    expect(page.get_by_role("switch", name="trace")).to_be_checked()
    expect(page.get_by_role("switch", name="loads")).to_be_checked()

    # other sections should NOT be checked
    expect(page.get_by_role("switch", name="overview")).not_to_be_checked()
    expect(page.get_by_role("switch", name="schema")).not_to_be_checked()
    expect(page.get_by_role("switch", name="state")).not_to_be_checked()

    # navigate away to release the DuckDB lock before the next test's fixture
    _go_home(page)


def test_sections_query_param_all(page: Page, fruit_pipeline: Any):
    """All sections should open when all are specified in ?sections= query param."""
    page.goto(
        "http://localhost:2718/?pipeline=fruit_pipeline&sections=overview,schema,state,trace,loads"
    )

    # wait for the pipeline to load
    expect(page.get_by_role("switch", name="overview")).to_be_visible(timeout=20000)

    # all specified switches should be checked
    for section in ["overview", "schema", "state", "trace", "loads"]:
        expect(page.get_by_role("switch", name=section)).to_be_checked()

    _go_home(page)


def test_dataset_browser_shown_by_default(page: Page, fruit_pipeline: Any):
    """The dataset browser renders on open with no toggle to switch it on."""
    _open_pipeline(page, "fruit_pipeline")

    # the browse-data section is visible without any user interaction
    expect(page.get_by_text(app_strings.browse_data.title).first).to_be_visible(timeout=20000)
    # and there is no on/off switch for it (other sections still have one)
    expect(page.get_by_role("switch", name="data", exact=True)).to_have_count(0)

    _go_home(page)


def test_auto_select_most_recent_pipeline(page: Page):
    """Opening the dashboard with no pipeline in the URL selects the most recently run one."""
    test_port = 2721
    with isolated_workspace("pipelines"):
        older = dlt.pipeline(pipeline_name="older_pipeline", destination="duckdb")
        older.run(fruitshop_source().with_resources("customers"))
        # ensure a distinct trace mtime so the ordering is deterministic
        time.sleep(1)
        newer = dlt.pipeline(pipeline_name="newer_pipeline", destination="duckdb")
        newer.run(fruitshop_source().with_resources("inventory"))

        with start_dashboard(port=test_port):
            page.goto(f"http://localhost:{test_port}")
            # the most recently run pipeline is selected automatically, no picking required
            expect(
                page.get_by_role("heading", name=re.compile(r"Pipeline\s+newer_pipeline"))
            ).to_be_visible(timeout=20000)


def test_no_pipelines_home(page: Page):
    """With no pipelines, show a hint and hide the empty pipeline dropdown."""
    test_port = 2722
    with isolated_workspace("pipelines"):
        with start_dashboard(port=test_port):
            page.goto(f"http://localhost:{test_port}")
            # the no-pipelines hint is shown
            expect(page.get_by_text("No pipelines found yet").first).to_be_visible(timeout=20000)
            # the (empty) pipeline dropdown is hidden
            expect(page.get_by_text(app_strings.app_pipeline_select_label)).to_have_count(0)


def test_deselect_pipeline_keeps_selector(page: Page):
    """Clearing the pipeline keeps the dropdown and shows a neutral hint, not the empty landing."""
    test_port = 2723
    with isolated_workspace("pipelines"):
        alpha = dlt.pipeline(pipeline_name="alpha", destination="duckdb")
        alpha.run(fruitshop_source().with_resources("customers"))
        time.sleep(1)
        beta = dlt.pipeline(pipeline_name="beta", destination="duckdb")
        beta.run(fruitshop_source().with_resources("inventory"))

        with start_dashboard(port=test_port):
            page.goto(f"http://localhost:{test_port}")
            expect(page.get_by_role("heading", name=re.compile(r"Pipeline\s+beta"))).to_be_visible(
                timeout=20000
            )

            _toggle_pipeline(page, "beta")

            expect(page.get_by_text(app_strings.app_pipeline_select_label)).to_have_count(1)
            expect(page.get_by_text("No pipeline selected").first).to_be_visible(timeout=15000)
            expect(page.get_by_text("No pipelines found yet")).to_have_count(0)
            expect(page.get_by_text(app_strings.browse_data.title)).to_have_count(0)

            _toggle_pipeline(page, "alpha")
            expect(page.get_by_role("heading", name=re.compile(r"Pipeline\s+alpha"))).to_be_visible(
                timeout=15000
            )


def test_switch_pipeline_no_error_flash(page: Page):
    """Switching A->B (a transient empty selection) lands on B with no error or stale hint."""
    test_port = 2724
    with isolated_workspace("pipelines"):
        alpha = dlt.pipeline(pipeline_name="alpha", destination="duckdb")
        alpha.run(fruitshop_source().with_resources("customers"))
        time.sleep(1)
        beta = dlt.pipeline(pipeline_name="beta", destination="duckdb")
        beta.run(fruitshop_source().with_resources("inventory"))

        with start_dashboard(port=test_port):
            page.goto(f"http://localhost:{test_port}")
            expect(page.get_by_role("heading", name=re.compile(r"Pipeline\s+beta"))).to_be_visible(
                timeout=20000
            )

            _toggle_pipeline(page, "alpha")

            expect(page.get_by_role("heading", name=re.compile(r"Pipeline\s+alpha"))).to_be_visible(
                timeout=15000
            )
            expect(
                page.get_by_text(app_strings.home_error_attach_pipeline.format("alpha"))
            ).to_have_count(0)
            expect(page.get_by_text("No pipeline selected")).to_have_count(0)


def test_empty_workspace_bad_url_deselect_shows_no_pipelines(page: Page):
    """An empty workspace opened with a bad ?pipeline= must show the no-pipelines landing once cleared."""
    test_port = 2726
    with isolated_workspace("pipelines"):
        with start_dashboard(port=test_port):
            page.goto(f"http://localhost:{test_port}/?pipeline=ghost")
            expect(
                page.get_by_text(app_strings.home_error_attach_pipeline.format("ghost")).first
            ).to_be_visible(timeout=20000)

            _toggle_pipeline(page, "ghost")

            expect(page.get_by_text("No pipelines found yet").first).to_be_visible(timeout=15000)
            expect(page.get_by_text("No pipeline selected")).to_have_count(0)
            expect(page.get_by_text(app_strings.app_pipeline_select_label)).to_have_count(0)


def test_bad_pipeline_url_keeps_selector(page: Page):
    """An unknown ?pipeline= shows an attach error but keeps the dropdown so the user recovers."""
    test_port = 2725
    with isolated_workspace("pipelines"):
        beta = dlt.pipeline(pipeline_name="beta", destination="duckdb")
        beta.run(fruitshop_source().with_resources("inventory"))

        with start_dashboard(port=test_port):
            page.goto(f"http://localhost:{test_port}/?pipeline=ghost")
            expect(
                page.get_by_text(app_strings.home_error_attach_pipeline.format("ghost")).first
            ).to_be_visible(timeout=20000)
            expect(page.get_by_text(app_strings.app_pipeline_select_label)).to_have_count(1)
            expect(page.get_by_role("button", name=app_strings.app_refresh_button)).to_have_count(0)

            _toggle_pipeline(page, "beta")
            expect(page.get_by_role("heading", name=re.compile(r"Pipeline\s+beta"))).to_be_visible(
                timeout=15000
            )
