from typing import Iterator
import pytest
from os import environ

import dlt
from dlt.common import json
from dlt.common.configuration.container import Container
from dlt.common.pipeline import LoadInfo, PipelineContext

from tests.utils import TEST_STORAGE_ROOT

PIPELINE_TEST_CASES_PATH = "./tests/pipeline/cases/"


@pytest.fixture(autouse=True)
def drop_dataset_from_env() -> None:
    if "DATASET_NAME" in environ:
        del environ["DATASET_NAME"]


@pytest.fixture(autouse=True)
def drop_pipeline() -> Iterator[None]:
    container = Container()
    if container[PipelineContext].is_active():
        container[PipelineContext].deactivate()
    yield
    if container[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()
        p._wipe_working_folder()
        # deactivate context
        container[PipelineContext].deactivate()


def assert_load_info(info: LoadInfo, expected_load_packages: int = 1) -> None:
    """Asserts that expected number of packages was loaded and there are no failed jobs"""
    assert len(info.loads_ids) == expected_load_packages
    # all packages loaded
    assert all(p.completed_at is not None for p in info.load_packages) is True
    # no failed jobs in any of the packages
    assert all(len(p.jobs["failed_jobs"]) == 0 for p in info.load_packages) is True


def json_case_path(name: str) -> str:
    return f"{PIPELINE_TEST_CASES_PATH}{name}.json"


def load_json_case(name: str) -> dict:
    with open(json_case_path(name), "br") as f:
        return json.load(f)
