from typing import Iterator
import pytest
from os import environ

import dlt
from dlt.common import json
from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext

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


def json_case_path(name: str) -> str:
    return f"{PIPELINE_TEST_CASES_PATH}{name}.json"


def load_json_case(name: str) -> dict:
    with open(json_case_path(name), "br") as f:
        return json.load(f)
