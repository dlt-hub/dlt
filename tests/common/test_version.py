import os
import pytest
from importlib.metadata import PackageNotFoundError
from packaging.requirements import Requirement
from types import SimpleNamespace
from typing import List
from unittest.mock import patch

from dlt.version import (
    get_installed_requirement_string,
    get_dependency_requirement,
    get_extra_requirements,
)


pytestmark = pytest.mark.serial


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


def test_get_extra_requirements() -> None:
    reqs = get_extra_requirements("s3")
    assert set(reqs) == {"s3fs", "botocore"}
    assert all(req.marker is None for req in reqs.values())
    # dlt's own floor keeps s3fs off the 2020 release that an unbounded spec resolves to
    assert not reqs["s3fs"].specifier.contains("0.4.2")
    assert get_extra_requirements("not-an-extra") == {}


@pytest.mark.parametrize(
    "declared, expected",
    [
        ('s3fs>=2022.4.0 ; extra == "s3"', ["s3fs>=2022.4.0"]),
        ('pyarrow>=16 ; python_version >= "3.0" and extra == "s3"', ["pyarrow>=16"]),
        ('pyarrow>=16 ; python_version < "3.0" and extra == "s3"', []),
        ('gcsfs>=2022 ; extra == "gs"', []),
        ('tomlkit ; python_version >= "3.0"', []),
    ],
    ids=[
        "extra-marker",
        "marker-for-this-python",
        "marker-for-another-python",
        "other-extra",
        "core-dep-with-marker",
    ],
)
def test_get_extra_requirements_selects_by_marker(declared: str, expected: List[str]) -> None:
    requires = [declared, 'botocore>=1.28 ; extra == "s3"', "requests>=2"]
    with patch("dlt.version.pkg_distribution", return_value=SimpleNamespace(requires=requires)):
        reqs = get_extra_requirements("s3")
    assert [str(req) for req in reqs.values()] == expected + ["botocore>=1.28"]
