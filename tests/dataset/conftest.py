from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from tests.utils import (
    preserve_environ,
    autouse_test_storage,
    auto_test_run_context,
    deactivate_pipeline,
)

if TYPE_CHECKING:
    import pathlib


@pytest.fixture(scope="module")
def module_tmp_path(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    """Temporary directory that persist for the lifetime of test `.py` file."""
    return tmp_path_factory.mktemp("pytest-test_relation")
