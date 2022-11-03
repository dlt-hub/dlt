from typing import Iterator
import pytest
from unittest.mock import patch

from dlt.common import pendulum
from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id

from dlt.load.exceptions import LoadClientTerminalInnerException
from dlt.destinations.redshift.redshift import RedshiftClient, psycopg2

from tests.utils import TEST_STORAGE_ROOT, delete_test_storage, skipifpypy
from tests.load.utils import expect_load_file, prepare_table, yield_client_with_storage


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_test_storage()


@pytest.fixture(scope="module")
def client() -> Iterator[RedshiftClient]:
    yield from yield_client_with_storage("redshift")


@skipifpypy
def test_text_too_long(client: RedshiftClient, file_storage: FileStorage) -> None:
    caps = client.capabilities()

    user_table_name = prepare_table(client)
    # insert string longer than redshift maximum
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
    # try some unicode value - redshift checks the max length based on utf-8 representation, not the number of characters
    # max_len_str = 'उ' * (65535 // 3) + 1 -> does not fit
    # max_len_str = 'a' * 65535 + 1 -> does not fit
    max_len_str = 'उ' * ((caps["max_text_data_type_length"] // 3) + 1)
    # max_len_str_b = max_len_str.encode("utf-8")
    # print(len(max_len_str_b))
    row_id = uniq_id()
    insert_values = f"('{row_id}', '{uniq_id()}', '{max_len_str}' , '{str(pendulum.now())}');"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.StringDataRightTruncation


def test_wei_value(client: RedshiftClient, file_storage: FileStorage) -> None:
    user_table_name = prepare_table(client)

    # max redshift decimal is (38, 0) (128 bit) = 10**38 - 1
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, parse_data__metadata__rasa_x_id)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {10**38});"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.InternalError_


@pytest.mark.skip
@skipifpypy
def test_maximum_query_size(client: RedshiftClient, file_storage: FileStorage) -> None:
    mocked_caps = RedshiftClient.capabilities()
    # this guarantees that we cross the redshift query limit
    mocked_caps["max_query_length"] = 2 * 20 * 1024 * 1024

    with patch.object(RedshiftClient, "capabilities") as caps:
        caps.return_value = mocked_caps

        insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
        insert_values = "('{}', '{}', '90238094809sajlkjxoiewjhduuiuehd', '{}'){}"
        insert_sql = insert_sql + insert_values.format(uniq_id(), uniq_id(), str(pendulum.now()), ",\n") * 150000
        insert_sql += insert_values.format(uniq_id(), uniq_id(), str(pendulum.now()), ";")

        user_table_name = prepare_table(client)
        with pytest.raises(LoadClientTerminalInnerException) as exv:
            expect_load_file(client, file_storage, insert_sql, user_table_name)
        # psycopg2.errors.SyntaxError: Statement is too large. Statement Size: 20971754 bytes. Maximum Allowed: 16777216 bytes
        assert type(exv.value.inner_exc) is psycopg2.errors.SyntaxError