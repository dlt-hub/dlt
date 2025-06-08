import pytest

from dlt.destinations.impl.cratedb.configuration import CrateDbCredentials


@pytest.fixture
def credentials() -> CrateDbCredentials:
    creds = CrateDbCredentials()
    creds.username = "crate"
    creds.password = ""
    creds.host = "localhost"
    return creds
