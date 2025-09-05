from typing import Any, Literal

import dlt
import pytest

from playwright.sync_api import Page, expect

from tests.utils import (
    patch_home_dir,
    autouse_test_storage,
    preserve_environ,
    wipe_pipeline,
)

from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)

from dlt import Schema

from dlt.helpers.dashboard import strings as app_strings


@pytest.fixture()
def simple_incremental_pipeline() -> Any:
    po = dlt.pipeline(pipeline_name="one_two_three", destination="duckdb")

    @dlt.resource(table_name="one_two_three")
    def resource(inc_id=dlt.sources.incremental("id")):
        yield [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}, {"id": 3, "name": "three"}]
        yield [{"id": 4, "name": "four"}, {"id": 5, "name": "five"}, {"id": 6, "name": "six"}]
        yield [{"id": 7, "name": "seven"}, {"id": 8, "name": "eight"}, {"id": 9, "name": "nine"}]

    po.run(resource())
    return po


@pytest.fixture()
def fruit_pipeline() -> Any:
    pf = dlt.pipeline(pipeline_name="fruit_pipeline", destination="duckdb")
    pf.run(fruitshop_source())
    return pf


@pytest.fixture()
def never_run_pipeline() -> Any:
    return dlt.pipeline(pipeline_name="never_run_pipeline", destination="duckdb")


@pytest.fixture()
def no_destination_pipeline() -> Any:
    pnd = dlt.pipeline(pipeline_name="no_destination_pipeline")
    pnd.extract(fruitshop_source())
    return pnd


@pytest.fixture()
def multi_schema_pipeline() -> Any:
    pms = dlt.pipeline(pipeline_name="multi_schema_pipeline", destination="duckdb")
    pms.run(
        fruitshop_source().with_resources("customers"), schema=Schema(name="fruitshop_customers")
    )
    pms.run(
        fruitshop_source().with_resources("inventory"), schema=Schema(name="fruitshop_inventory")
    )
    pms.run(
        fruitshop_source().with_resources("purchases"), schema=Schema(name="fruitshop_purchases")
    )
    return pms


@pytest.fixture()
def failed_pipeline() -> Any:
    fp = dlt.pipeline(
        pipeline_name="failed_pipeline",
        destination="duckdb",
    )

    @dlt.resource
    def broken_resource():
        raise AssertionError("I am broken")

    with pytest.raises(Exception):
        fp.run(broken_resource())
    return fp


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
    expect(page).to_have_title("dlt pipeline dashboard")

    # check top heading
    expect(
        page.get_by_role("heading", name="Welcome to the dltHub pipeline dashboard...")
    ).to_contain_text(
        "Welcome to the dltHub pipeline dashboard..."
    )  #

    #
    # Exception pipeline
    #


def test_exception_pipeline(page: Page, failed_pipeline: Any):
    _go_home(page)
    page.get_by_role("link", name="failed_pipeline").click()

    # overview page
    _open_section(page, "overview")
    expect(page.get_by_text("_storage/.dlt/pipelines/failed_pipeline")).to_be_visible()
    expect(
        page.get_by_text("Exception encountered during last pipeline run in step").nth(0)
    ).to_be_visible()

    _open_section(page, "schema")
    expect(page.get_by_text(app_strings.schema_no_default_available_text[0:20])).to_be_visible()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.schema_no_default_available_text[0:20])).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text("dataset_name: failed_pipeline_dataset")).to_be_visible()

    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace_subtitle)).to_be_visible()
    expect(
        page.get_by_text("Exception encountered during last pipeline run in step").nth(0)
    ).to_be_visible()

    # loads page
    _open_section(page, "loads")
    expect(page.get_by_text(app_strings.loads_loading_failed_text[0:20])).to_be_visible()

    _open_section(page, "ibis")
    expect(page.get_by_text(app_strings.ibis_backend_error_text[0:20])).to_be_visible()


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

        schema_selector = page.get_by_role("combobox")
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
    expect(page.get_by_text("_storage/.dlt/pipelines/one_two_three")).to_be_visible()

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

    # check state (we check some info from the incremental state here)
    page.get_by_text("Show source and resource state").click()
    expect(
        page.get_by_label("Show source and resource").get_by_text("incremental:")
    ).to_be_visible()
    expect(
        page.get_by_label("Show source and resource").get_by_text("last_value: 9")
    ).to_be_visible()

    page.get_by_role("button", name="Run Query").click()

    # enable dlt tables
    page.get_by_role("switch", name="Show _dlt tables").check()

    # state page
    _open_section(page, "state")
    expect(
        page.get_by_text("dataset_name: one_two_three_dataset")
    ).to_be_visible()  # this is part of the state yaml

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
    _open_section(page, "ibis")
    expect(page.get_by_text(app_strings.ibis_backend_connected_text)).to_be_visible()


def test_fruit_pipeline(page: Page, fruit_pipeline: Any):
    # check fruit pipeline
    _go_home(page)
    page.get_by_role("link", name="fruit_pipeline").click()

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
    expect(page.get_by_text("dataset_name: fruit_pipeline_dataset")).to_be_visible()

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
    _open_section(page, "ibis")
    expect(page.get_by_text(app_strings.ibis_backend_connected_text)).to_be_visible()


def test_never_run_pipeline(page: Page, never_run_pipeline: Any):
    _go_home(page)
    page.get_by_role("link", name="never_run_pipeline").click()

    expect(page.get_by_text("_storage/.dlt/pipelines/never_run_pipeline")).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    expect(page.get_by_text(app_strings.schema_no_default_available_text[0:20])).to_be_visible()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.schema_no_default_available_text[0:20])).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text("dataset_name: never_run_pipeline_dataset")).to_be_visible()

    _open_section(page, "trace")
    expect(page.get_by_text(app_strings.trace_subtitle)).to_be_visible()
    expect(page.get_by_text(app_strings.trace_no_trace_text.strip()).nth(0)).to_be_visible()

    # loads page
    _open_section(page, "loads")
    expect(page.get_by_text(app_strings.loads_loading_failed_text[0:20])).to_be_visible()

    _open_section(page, "ibis")
    expect(page.get_by_text(app_strings.ibis_backend_error_text[0:20])).to_be_visible()


def test_no_destination_pipeline(page: Page, no_destination_pipeline: Any):
    # check no destination pipeline
    _go_home(page)
    page.get_by_role("link", name="no_destination_pipeline").click()

    expect(page.get_by_text("_storage/.dlt/pipelines/no_destination_pipeline")).to_be_visible()

    # check schema info (this is the yaml part)
    _open_section(page, "schema")
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("name: fruitshop").nth(1)).to_be_attached()

    # browse data
    _open_section(page, "data")
    expect(page.get_by_text(app_strings.browse_data_error_text[0:20])).to_be_visible()

    _open_section(page, "state")
    expect(page.get_by_text("dataset_name: null")).to_be_visible()

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

    _open_section(page, "ibis")
    expect(page.get_by_text(app_strings.ibis_backend_error_text[0:20])).to_be_visible()
