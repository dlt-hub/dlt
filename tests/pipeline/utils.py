from typing import Iterator
import pytest
from os import environ
from unittest.mock import patch

import dlt
from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext

from tests.utils import TEST_STORAGE_ROOT


@pytest.fixture(autouse=True)
def drop_dataset_from_env() -> None:
    if "DATASET_NAME" in environ:
        del environ["DATASET_NAME"]


@pytest.fixture(autouse=True)
def patch_working_dir() -> None:
    with patch("dlt.common.pipeline._get_home_dir") as _get_home_dir:
        _get_home_dir.return_value = TEST_STORAGE_ROOT
        yield


@pytest.fixture(autouse=True)
def drop_pipeline() -> Iterator[None]:
    yield
    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()
        p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()
