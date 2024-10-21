import base64
import os
from typing import Iterator
import pytest
from unittest.mock import patch

from dlt.common import json, pendulum
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import VERSION_TABLE_NAME
from dlt.common.storages import FileStorage
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.utils import uniq_id

from dlt.destinations.exceptions import DatabaseTerminalException
from dlt.destinations import redshift
from dlt.destinations.impl.redshift.configuration import (
    RedshiftCredentials,
    RedshiftClientConfiguration,
)
from dlt.destinations.impl.redshift.redshift import RedshiftClient, psycopg2

from tests.common.utils import COMMON_TEST_CASES_PATH
from tests.utils import TEST_STORAGE_ROOT, autouse_test_storage, skipifpypy
from tests.load.utils import expect_load_file, prepare_table, yield_client_with_storage

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(scope="function")
def client() -> Iterator[RedshiftClient]:
    yield from yield_client_with_storage("redshift")  # type: ignore[misc]


def test_postgres_and_redshift_credentials_defaults() -> None:
    red_cred = RedshiftCredentials()
    assert red_cred.port == 5439
    assert red_cred.connect_timeout == 15
    assert RedshiftCredentials.__config_gen_annotations__ == ["port", "connect_timeout"]
    resolve_configuration(red_cred, explicit_value="postgres://loader:loader@localhost/dlt_data")
    assert red_cred.port == 5439


def test_redshift_factory() -> None:
    schema = Schema("schema")
    dest = redshift()
    client = dest.client(schema, RedshiftClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.staging_iam_role is None
    assert client.config.has_case_sensitive_identifiers is False
    assert client.capabilities.has_case_sensitive_identifiers is False
    assert client.capabilities.casefold_identifier is str.lower

    # set args explicitly
    dest = redshift(has_case_sensitive_identifiers=True, staging_iam_role="LOADER")
    client = dest.client(schema, RedshiftClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.staging_iam_role == "LOADER"
    assert client.config.has_case_sensitive_identifiers is True
    assert client.capabilities.has_case_sensitive_identifiers is True
    assert client.capabilities.casefold_identifier is str

    # set args via config
    os.environ["DESTINATION__STAGING_IAM_ROLE"] = "LOADER"
    os.environ["DESTINATION__HAS_CASE_SENSITIVE_IDENTIFIERS"] = "True"
    dest = redshift()
    client = dest.client(schema, RedshiftClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.staging_iam_role == "LOADER"
    assert client.config.has_case_sensitive_identifiers is True
    assert client.capabilities.has_case_sensitive_identifiers is True
    assert client.capabilities.casefold_identifier is str


@skipifpypy
def test_text_too_long(client: RedshiftClient, file_storage: FileStorage) -> None:
    caps = client.capabilities

    user_table_name = prepare_table(client)
    # insert string longer than redshift maximum
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
    # try some unicode value - redshift checks the max length based on utf-8 representation, not the number of characters
    # max_len_str = 'उ' * (65535 // 3) + 1 -> does not fit
    # max_len_str = 'a' * 65535 + 1 -> does not fit
    max_len_str = "उ" * ((caps["max_text_data_type_length"] // 3) + 1)
    # max_len_str_b = max_len_str.encode("utf-8")
    # print(len(max_len_str_b))
    row_id = uniq_id()
    insert_values = f"('{row_id}', '{uniq_id()}', '{max_len_str}' , '{str(pendulum.now())}');"
    job = expect_load_file(
        client, file_storage, insert_sql + insert_values, user_table_name, "failed"
    )
    assert type(job._exception.dbapi_exception) is psycopg2.errors.StringDataRightTruncation  # type: ignore


def test_wei_value(client: RedshiftClient, file_storage: FileStorage) -> None:
    user_table_name = prepare_table(client)

    # max redshift decimal is (38, 0) (128 bit) = 10**38 - 1
    insert_sql = (
        "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp,"
        " parse_data__metadata__rasa_x_id)\nVALUES\n"
    )
    insert_values = (
        f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd',"
        f" '{str(pendulum.now())}', {10**38});"
    )
    job = expect_load_file(
        client, file_storage, insert_sql + insert_values, user_table_name, "failed"
    )
    assert type(job._exception.dbapi_exception) is psycopg2.errors.InternalError_  # type: ignore


def test_schema_string_exceeds_max_text_length(client: RedshiftClient) -> None:
    client.update_stored_schema()
    # schema should be compressed and stored as base64
    schema = SchemaStorage.load_schema_file(
        os.path.join(COMMON_TEST_CASES_PATH, "schemas/ev1"), "event", ("json",)
    )
    schema_str = json.dumps(schema.to_dict())
    assert len(schema_str.encode("utf-8")) > client.capabilities.max_text_data_type_length
    client._update_schema_in_storage(schema)
    schema_info = client.get_stored_schema(client.schema.name)
    assert schema_info.schema == schema_str
    # take base64 from db
    with client.sql_client.execute_query(
        f"SELECT schema FROM {VERSION_TABLE_NAME} WHERE version_hash ="
        f" '{schema.stored_version_hash}'"
    ) as cur:
        row = cur.fetchone()
    # decode base
    base64.b64decode(row[0], validate=True)


@pytest.mark.skip
@skipifpypy
def test_maximum_query_size(client: RedshiftClient, file_storage: FileStorage) -> None:
    mocked_caps = RedshiftClient.capabilities
    # this guarantees that we cross the redshift query limit
    mocked_caps["max_query_length"] = 2 * 20 * 1024 * 1024

    with patch.object(RedshiftClient, "capabilities") as caps:
        caps.return_value = mocked_caps

        insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
        insert_values = "('{}', '{}', '90238094809sajlkjxoiewjhduuiuehd', '{}'){}"
        insert_sql = (
            insert_sql
            + insert_values.format(uniq_id(), uniq_id(), str(pendulum.now()), ",\n") * 150000
        )
        insert_sql += insert_values.format(uniq_id(), uniq_id(), str(pendulum.now()), ";")

        user_table_name = prepare_table(client)
        with pytest.raises(DatabaseTerminalException) as exv:
            expect_load_file(client, file_storage, insert_sql, user_table_name)
        # psycopg2.errors.SyntaxError: Statement is too large. Statement Size: 20971754 bytes. Maximum Allowed: 16777216 bytes
        assert type(exv.value.dbapi_exception) is psycopg2.errors.SyntaxError
