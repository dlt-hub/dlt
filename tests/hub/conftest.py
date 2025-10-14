from typing import Iterator
import pytest

from dlthub.common.license import LicenseContext

from tests.hub.utils import issue_ephemeral_license


@pytest.fixture(scope="session", autouse=True)
def auto_issue_license() -> Iterator[LicenseContext]:
    yield from issue_ephemeral_license()
