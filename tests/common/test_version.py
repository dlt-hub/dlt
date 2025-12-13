import os
import pytest
from importlib.metadata import PackageNotFoundError
from packaging.requirements import Requirement

from dlt.version import get_installed_requirement_string, get_dependency_requirement


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


def test_get_dependency_requirement() -> None:
    # dlt depends on dlthub, so this should return a Requirement
    req = get_dependency_requirement("dlthub")
    assert req is not None
    assert isinstance(req, Requirement)
    assert req.name == "dlthub"
    # click has a version specifier
    assert str(req.specifier) != ""

    # dlt depends on fsspec with a version constraint
    req = get_dependency_requirement("fsspec")
    assert req is not None
    assert req.name == "fsspec"
    # verify we can check version satisfaction
    assert "2022.4.0" in req.specifier

    # non-existent dependency returns None
    req = get_dependency_requirement("non-existent-package-xyz")
    assert req is None
