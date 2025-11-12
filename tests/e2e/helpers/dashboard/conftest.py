import pathlib
import sys
from typing import Any
import time
import urllib.request
import multiprocessing as mp

import psutil
from dlt._workspace.helpers.dashboard.runner import kill_dashboard, run_dashboard
import pytest

import dlt
from dlt._workspace._templates._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)


def _normpath(path: str) -> str:
    """normalize path to unix style and lowercase for windows tests"""
    return str(pathlib.Path(path)) if sys.platform.startswith("win") else path


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
        fruitshop_source().with_resources("customers"),
        schema=dlt.Schema(name="fruitshop_customers"),
    )
    pms.run(
        fruitshop_source().with_resources("inventory"),
        schema=dlt.Schema(name="fruitshop_inventory"),
    )
    pms.run(
        fruitshop_source().with_resources("purchases"),
        schema=dlt.Schema(name="fruitshop_purchases"),
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


def _wait_http_up(url: str, timeout_s: float = 15.0) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            with urllib.request.urlopen(url, timeout=0.2):
                return
        except Exception:
            time.sleep(0.1)
    raise TimeoutError(f"Server did not become ready: {url}")


def dashboard_child(pipelines_dir: str, port: int = 2718, test_identifiers: bool = True) -> None:
    run_dashboard(
        pipeline_name=None,
        edit=False,
        pipelines_dir=pipelines_dir,
        port=port,
        host="127.0.0.1",
        with_test_identifiers=test_identifiers,
    )


@pytest.fixture(scope="module", autouse=True)
def start_dashboard_server():
    start_dashboard(pipelines_dir=_normpath("_storage/.dlt/pipelines"))
    yield
    kill_dashboard()


@pytest.fixture()
def kill_dashboard_for_test():
    yield
    kill_dashboard(port=2719)


def start_dashboard(
    pipelines_dir: str = None, port: int = 2718, test_identifiers: bool = True
) -> None:
    ctx = mp.get_context("spawn")
    proc = ctx.Process(
        target=dashboard_child, args=(pipelines_dir, port, test_identifiers), daemon=True
    )
    proc.start()
    _wait_http_up(f"http://127.0.0.1:{port}", timeout_s=20.0)
