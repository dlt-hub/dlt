import os
import pytest
from importlib.metadata import PackageNotFoundError

from dlt.version import get_installed_requirement_string


def test_installed_requirement_string() -> None:
    # we are running tests in editable mode so we should get path to here
    path = get_installed_requirement_string()
    assert os.path.commonpath((__file__, path)) == path
    # requests should be properly installed
    requirement = get_installed_requirement_string("requests")
    assert requirement.startswith("requests==")
    # this is not installed
    with pytest.raises(PackageNotFoundError):
        get_installed_requirement_string("requests-X")
