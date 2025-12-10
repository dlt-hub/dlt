import pathlib
import sys
from typing import Any
import pickle
import os

import pytest

import dlt
from dlt._workspace.helpers.dashboard.runner import start_dashboard
from dlt._workspace._templates._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)
from dlt._workspace.helpers.dashboard import utils as dashboard_utils
from dlt.pipeline.trace import get_trace_file_path


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


@pytest.fixture()
def broken_trace_pipeline() -> Any:
    """Pipeline with a deliberately corrupted trace file to test dashboard robustness."""
    bp = dlt.pipeline(
        pipeline_name="broken_trace_pipeline",
        destination="duckdb",
    )
    bp.run(fruitshop_source())

    trace_file = get_trace_file_path(bp.pipelines_dir, bp.pipeline_name)
    os.makedirs(os.path.dirname(trace_file), exist_ok=True)
    with open(trace_file, mode="wb") as f:
        pickle.dump({"not": "a real PipelineTrace"}, f)

    return bp


@pytest.fixture(scope="module", autouse=True)
def start_dashboard_server():
    with start_dashboard(pipelines_dir=_normpath("_storage/.dlt/pipelines")) as proc:
        yield proc
