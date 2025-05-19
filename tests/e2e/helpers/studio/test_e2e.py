from typing import Any

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


def test_page_loads(page: Page):
    page.goto("http://localhost:2718")

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
    expect(page.get_by_text("Pipeline state synced successfully from duckdb")).to_be_visible()
    expect(page.get_by_text("_storage/.dlt/pipelines/one_two_three")).to_be_visible()

    # check schema info (this is the yaml part)
    page.get_by_role("tab", name="Schema").click()
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("name: one_two_three").nth(1)).to_be_attached()

    # browse data
    page.get_by_role("tab", name="Browse Data").click()
    expect(page.get_by_text("Last successful query result").nth(1)).to_be_visible()

    page.get_by_role("tab", name="State").click()
    expect(page.get_by_text('"dataset_name": "one_two_three_dataset"')).to_be_visible()

    page.get_by_role("tab", name="Ibis").click()
    expect(page.get_by_text("Ibis Backend connected successfully.")).to_be_visible()

    #
    # Fruit pipeline
    #

    # check fruit pipeline
    page.goto("http://localhost:2718")
    page.get_by_role("link", name="fruit_pipeline").click()

    expect(page.get_by_text("Pipeline state synced successfully from duckdb")).to_be_visible()
    expect(page.get_by_text("_storage/.dlt/pipelines/fruit_pipeline")).to_be_visible()

    # check schema info (this is the yaml part)
    page.get_by_role("tab", name="Schema").click()
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("name: fruitshop").nth(1)).to_be_attached()

    # browse data
    page.get_by_role("tab", name="Browse Data").click()
    expect(page.get_by_text("Last successful query result").nth(1)).to_be_visible()

    page.get_by_role("tab", name="State").click()
    expect(page.get_by_text('"dataset_name": "fruit_pipeline_dataset"')).to_be_visible()

    page.get_by_role("tab", name="Ibis").click()
    expect(page.get_by_text("Ibis Backend connected successfully.")).to_be_visible()

    #
    # Never run pipeline
    #

    page.goto("http://localhost:2718")
    page.get_by_role("link", name="never_run_pipeline").click()

    expect(page.get_by_text("Error syncing pipeline from destination.")).to_be_visible()
    expect(page.get_by_text("_storage/.dlt/pipelines/never_run_pipeline")).to_be_visible()

    # check schema info (this is the yaml part)
    page.get_by_role("tab", name="Schema").click()
    expect(page.get_by_text("No Schema available")).to_be_visible()

    # browse data
    page.get_by_role("tab", name="Browse Data").click()
    expect(page.get_by_text("Error connecting to destination")).to_be_visible()

    page.get_by_role("tab", name="State").click()
    expect(page.get_by_text('"dataset_name": "never_run_pipeline_dataset"')).to_be_visible()

    page.get_by_role("tab", name="Ibis").click()
    expect(page.get_by_text("Error connecting to Ibis Backend")).to_be_visible()

    # check no destination pipeline
    page.goto("http://localhost:2718")
    page.get_by_role("link", name="no_destination_pipeline").click()

    expect(page.get_by_text("Error syncing pipeline from destination.")).to_be_visible()
    expect(page.get_by_text("_storage/.dlt/pipelines/no_destination_pipeline")).to_be_visible()

    # check schema info (this is the yaml part)
    page.get_by_role("tab", name="Schema").click()
    page.get_by_text("Show raw schema as yaml").click()
    expect(page.get_by_text("name: fruitshop").nth(1)).to_be_attached()

    # browse data
    page.get_by_role("tab", name="Browse Data").click()
    expect(page.get_by_text("Error connecting to destination")).to_be_visible()

    page.get_by_role("tab", name="State").click()
    expect(page.get_by_text('"dataset_name": null')).to_be_visible()

    page.get_by_role("tab", name="Ibis").click()
    expect(page.get_by_text("Error connecting to Ibis Backend")).to_be_visible()
