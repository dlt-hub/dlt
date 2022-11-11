from typing import Iterator
import pytest

from dlt.common import pendulum, Wei
from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id

from dlt.destinations.postgres.postgres import PostgresClient, psycopg2

from tests.utils import TEST_STORAGE_ROOT, delete_test_storage, skipifpypy
from tests.load.utils import expect_load_file, prepare_table, yield_client_with_storage


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_test_storage()


@pytest.fixture(scope="module")
def client() -> Iterator[PostgresClient]:
    yield from yield_client_with_storage("postgres")


def test_wei_value(client: PostgresClient, file_storage: FileStorage) -> None:
    user_table_name = prepare_table(client)

    # postgres supports EVM precisions
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, parse_data__metadata__rasa_x_id)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {Wei.from_int256(2*256-1)});"
    expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)

    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, parse_data__metadata__rasa_x_id)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {Wei.from_int256(2*256-1, 18)});"
    expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)

    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, parse_data__metadata__rasa_x_id)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {Wei.from_int256(2*256-1, 78)});"
    expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
