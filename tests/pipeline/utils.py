import pytest
from os import environ

import dlt
from dlt.common import json
from dlt.common.pipeline import LoadInfo, PipelineContext

from tests.utils import TEST_STORAGE_ROOT

PIPELINE_TEST_CASES_PATH = "./tests/pipeline/cases/"


@pytest.fixture(autouse=True)
def drop_dataset_from_env() -> None:
    if "DATASET_NAME" in environ:
        del environ["DATASET_NAME"]


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
    with open(json_case_path(name), "rb") as f:
        return json.load(f)


@dlt.source
def airtable_emojis():

    @dlt.resource(name="📆 Schedule")
    def schedule():

        yield [1, 2, 3]

    @dlt.resource(name="💰Budget", primary_key=("🔑book_id", "asset_id"))
    def budget():
        # return empty
        yield

    @dlt.resource(name="🦚Peacock", selected=False, primary_key="🔑id")
    def peacock():
        dlt.current.resource_state()["🦚🦚🦚"] = "🦚"
        yield [{"peacock": [1, 2, 3], "🔑id": 1}]

    @dlt.resource(name="🦚WidePeacock", selected=False)
    def wide_peacock():
        yield [{"peacock": [1, 2, 3]}]


    return budget, schedule, peacock, wide_peacock
