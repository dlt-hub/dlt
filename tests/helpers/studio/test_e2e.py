from typing import Any

import dlt
import time
import pytest
import os
import shutil

from playwright.sync_api import Page, expect

from tests.utils import (
    patch_home_dir,
    autouse_test_storage,
    preserve_environ,
    duckdb_pipeline_location,
    wipe_pipeline,
)

from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)


@pytest.fixture(scope="session", autouse=True)
def setup_pipelines() -> Any:
    # simple pipeline
    po = dlt.pipeline(pipeline_name="one_two_three", destination="duckdb")
    po.run([1, 2, 3], table_name="one_two_three")

    # fruit pipeline
    pf = dlt.pipeline(pipeline_name="fruit_pipeline", destination="duckdb")
    pf.run(fruitshop_source())

    # never run pipeline
    pn = dlt.pipeline(pipeline_name="never_run_pipeline", destination="duckdb")

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

    # check one two three pipeline
    page.get_by_role("link", name="one_two_three").click()
    page.get_by_role("tab", name="State").click()
    page.get_by_role("tab", name="Schema").click()
    page.get_by_role("tab", name="Browse Data").click()
    page.get_by_role("tab", name="Ibis").click()

    # check fruit pipeline
    page.goto("http://localhost:2718")
    page.get_by_role("link", name="fruit_pipeline").click()
    page.get_by_role("tab", name="State").click()
    page.get_by_role("tab", name="Schema").click()
    page.get_by_role("tab", name="Browse Data").click()
    page.get_by_role("tab", name="Ibis").click()

    # check fruit pipeline
    page.goto("http://localhost:2718?pipeline=never_run_pipeline")
    page.get_by_role("tab", name="State").click()
    page.get_by_role("tab", name="Schema").click()
    page.get_by_role("tab", name="Browse Data").click()
    page.get_by_role("tab", name="Ibis").click()

    # check no destination pipeline
    page.goto("http://localhost:2718?pipeline=no_destination_pipeline")
    page.get_by_role("tab", name="State").click()
    page.get_by_role("tab", name="Schema").click()
    page.get_by_role("tab", name="Browse Data").click()
    page.get_by_role("tab", name="Ibis").click()
