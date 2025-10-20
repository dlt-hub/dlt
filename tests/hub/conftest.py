from typing import Iterator, Any
import pytest


@pytest.fixture(scope="session", autouse=True)
def auto_issue_license() -> Iterator[Any]:
    # skip tests if module not installed
    pytest.importorskip("dlthub")

    from dlthub.common.license import LicenseContext

    from tests.hub.utils import issue_ephemeral_license

    yield from issue_ephemeral_license()
