import os
from typing import Iterator
import pytest

from dlt.common.configuration.resolve import (
    resolve_configuration,
    ConfigFieldMissingException,
)
from dlt.common.storages import FileStorage

from dlt.destinations.impl.cratedb.configuration import CrateDbCredentials
from dlt.destinations.impl.cratedb.cratedb import CrateDbClient

from tests.utils import TEST_STORAGE_ROOT, delete_test_storage
from tests.load.utils import yield_client_with_storage
from tests.common.configuration.utils import environment


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_test_storage()


@pytest.fixture(scope="function")
def client() -> Iterator[CrateDbClient]:
    yield from yield_client_with_storage("cratedb")  # type: ignore[misc]


def test_cratedb_credentials_defaults() -> None:
    pg_cred = CrateDbCredentials()
    assert pg_cred.port == 5432
    assert pg_cred.connect_timeout == 15
    assert pg_cred.client_encoding is None
    assert CrateDbCredentials.__config_gen_annotations__ == ["port", "connect_timeout"]
    # port should be optional
    resolve_configuration(pg_cred, explicit_value="cratedb://loader:loader@localhost/DLT_DATA")
    assert pg_cred.port == 5432
    # preserve case
    assert pg_cred.database is None


def test_cratedb_credentials_native_value(environment) -> None:
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(
            CrateDbCredentials(), explicit_value="cratedb://loader@localhost/dlt_data"
        )
    # set password via env
    os.environ["CREDENTIALS__PASSWORD"] = "pass"
    c = resolve_configuration(
        CrateDbCredentials(), explicit_value="cratedb://loader@localhost/dlt_data"
    )
    assert c.is_resolved()
    assert c.password == "pass"
    # but if password is specified - it is final
    c = resolve_configuration(
        CrateDbCredentials(),
        explicit_value="cratedb://loader:loader@localhost/dlt_data",
    )
    assert c.is_resolved()
    assert c.password == "loader"

    c = CrateDbCredentials("cratedb://loader:loader@localhost/dlt_data")
    assert c.password == "loader"
    assert c.database is None


def test_cratedb_query_params() -> None:
    dsn_full = (
        "cratedb://loader:pass@localhost:5432/dlt_data?client_encoding=utf-8&connect_timeout=600"
    )
    dsn_nodb = "cratedb://loader:pass@localhost:5432?client_encoding=utf-8&connect_timeout=600"
    csc = CrateDbCredentials()
    csc.parse_native_representation(dsn_full)
    assert csc.connect_timeout == 600
    assert csc.client_encoding == "utf-8"
    assert csc.to_native_representation() == dsn_nodb
