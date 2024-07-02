import os
from typing import Iterator
import pytest

from dlt.common import pendulum, Wei
from dlt.common.configuration.resolve import resolve_configuration, ConfigFieldMissingException
from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id

from dlt.destinations.impl.postgres.configuration import PostgresCredentials
from dlt.destinations.impl.postgres.postgres import PostgresClient
from dlt.destinations.impl.postgres.sql_client import psycopg2

from tests.utils import TEST_STORAGE_ROOT, delete_test_storage, skipifpypy
from tests.load.utils import expect_load_file, prepare_table, yield_client_with_storage
from tests.common.configuration.utils import environment

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_test_storage()


@pytest.fixture(scope="function")
def client() -> Iterator[PostgresClient]:
    yield from yield_client_with_storage("postgres")  # type: ignore[misc]


def test_postgres_credentials_defaults() -> None:
    pg_cred = PostgresCredentials()
    assert pg_cred.port == 5432
    assert pg_cred.connect_timeout == 15
    assert PostgresCredentials.__config_gen_annotations__ == ["port", "connect_timeout"]
    # port should be optional
    resolve_configuration(pg_cred, explicit_value="postgres://loader:loader@localhost/DLT_DATA")
    assert pg_cred.port == 5432
    # preserve case
    assert pg_cred.database == "DLT_DATA"


def test_postgres_credentials_native_value(environment) -> None:
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(
            PostgresCredentials(), explicit_value="postgres://loader@localhost/dlt_data"
        )
    # set password via env
    os.environ["CREDENTIALS__PASSWORD"] = "pass"
    c = resolve_configuration(
        PostgresCredentials(), explicit_value="postgres://loader@localhost/dlt_data"
    )
    assert c.is_resolved()
    assert c.password == "pass"
    # but if password is specified - it is final
    c = resolve_configuration(
        PostgresCredentials(), explicit_value="postgres://loader:loader@localhost/dlt_data"
    )
    assert c.is_resolved()
    assert c.password == "loader"

    c = PostgresCredentials("postgres://loader:loader@localhost/dlt_data")
    assert c.password == "loader"
    assert c.database == "dlt_data"


def test_postgres_credentials_timeout() -> None:
    # test postgres timeout
    dsn = "postgres://loader:pass@localhost:5432/dlt_data?connect_timeout=600"
    csc = PostgresCredentials()
    csc.parse_native_representation(dsn)
    assert csc.connect_timeout == 600
    assert csc.to_native_representation() == dsn


def test_wei_value(client: PostgresClient, file_storage: FileStorage) -> None:
    user_table_name = prepare_table(client)

    # postgres supports EVM precisions
    insert_sql = (
        "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp,"
        " parse_data__metadata__rasa_x_id)\nVALUES\n"
    )
    insert_values = (
        f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd',"
        f" '{str(pendulum.now())}', {Wei.from_int256(2*256-1)});"
    )
    expect_load_file(client, file_storage, insert_sql + insert_values, user_table_name)

    insert_sql = (
        "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp,"
        " parse_data__metadata__rasa_x_id)\nVALUES\n"
    )
    insert_values = (
        f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd',"
        f" '{str(pendulum.now())}', {Wei.from_int256(2*256-1, 18)});"
    )
    expect_load_file(client, file_storage, insert_sql + insert_values, user_table_name)

    insert_sql = (
        "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp,"
        " parse_data__metadata__rasa_x_id)\nVALUES\n"
    )
    insert_values = (
        f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd',"
        f" '{str(pendulum.now())}', {Wei.from_int256(2*256-1, 78)});"
    )
    expect_load_file(client, file_storage, insert_sql + insert_values, user_table_name)
