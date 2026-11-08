from tests.utils import skip_if_not_active

skip_if_not_active("snowflake")

from copy import deepcopy
import os
from pathlib import Path
from typing import Callable, Iterator
from pytest_mock import MockerFixture
import pytest

import dlt
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.libs.cryptography import decode_private_key
from dlt.common.schema.schema import Schema
from dlt.common.utils import digest128, uniq_id
from dlt.destinations.impl.snowflake.configuration import SnowflakeCredentials
from dlt.destinations.impl.snowflake.snowflake import SUPPORTED_HINTS, SnowflakeClient
from dlt.destinations.job_client_impl import SqlJobClientBase

from dlt.destinations.sql_client import TQueryTags

from tests.cases import TABLE_UPDATE
from tests.pipeline.utils import assert_load_info
from tests.load.utils import yield_client_with_storage, cm_yield_client, empty_schema


# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

QUERY_TAG = (
    '{{"operation":"{operation}", "source":"{source}", "resource":"{resource}", "table": "{table}",'
    ' "load_id":"{load_id}", "pipeline_name":"{pipeline_name}"}}'
)
QUERY_TAGS_DICT: TQueryTags = {
    "operation": "load",
    "source": "test_source",
    "resource": "test_resource",
    "table": "test_table",
    "load_id": "1109291083091",
    "pipeline_name": "test_pipeline",
}


@pytest.fixture(scope="function")
def client() -> Iterator[SqlJobClientBase]:
    os.environ["QUERY_TAG"] = QUERY_TAG
    yield from yield_client_with_storage("snowflake")


def test_create_table_with_hints(client: SnowflakeClient, empty_schema: Schema) -> None:
    mod_update = deepcopy(TABLE_UPDATE[:11])
    # mock hints
    client.config.create_indexes = True
    client.active_hints = SUPPORTED_HINTS
    client.schema = empty_schema

    mod_update[0]["primary_key"] = True
    mod_update[5]["primary_key"] = True

    mod_update[0]["sort"] = True
    mod_update[4]["parent_key"] = True

    # unique constraints are always single columns
    mod_update[1]["unique"] = True
    mod_update[7]["unique"] = True

    sql = ";".join(client._get_table_update_sql("event_test_table", mod_update, False))

    print(sql)
    client.sql_client.execute_sql(sql)

    # generate alter table
    mod_update = deepcopy(TABLE_UPDATE[11:])
    mod_update[0]["primary_key"] = True
    mod_update[1]["unique"] = True

    sql = ";".join(client._get_table_update_sql("event_test_table", mod_update, True))

    print(sql)
    client.sql_client.execute_sql(sql)


def test_query_tag(client: SnowflakeClient, mocker: MockerFixture):
    assert client.config.query_tag == QUERY_TAG
    # make sure we generate proper query
    execute_sql_spy = mocker.spy(client.sql_client, "execute_sql")
    # reset the query if tags are not set
    client.sql_client.set_query_tags(None)
    execute_sql_spy.assert_called_once_with(sql="ALTER SESSION UNSET QUERY_TAG")
    execute_sql_spy.reset_mock()
    # set query tags
    client.sql_client.set_query_tags(QUERY_TAGS_DICT)
    execute_sql_spy.assert_called_once_with(
        sql=(
            'ALTER SESSION SET QUERY_TAG = \'{"operation":"load", "source":"test_source",'
            ' "resource":"test_resource", "table": "test_table", "load_id":"1109291083091",'
            ' "pipeline_name":"test_pipeline"}\''
        )
    )
    # remove query tag from config
    client.sql_client.query_tag = None
    execute_sql_spy.reset_mock()
    client.sql_client.set_query_tags(QUERY_TAGS_DICT)
    execute_sql_spy.assert_not_called()
    execute_sql_spy.reset_mock()
    client.sql_client.set_query_tags(None)
    execute_sql_spy.assert_not_called()


def test_session_autocommit() -> None:
    with cm_yield_client("snowflake", "dataset") as client:

        def _get_autocommit() -> bool:
            rows = client.sql_client.execute_sql("SHOW PARAMETERS LIKE 'autocommit' IN SESSION;")
            return rows[0][1].lower() == "true"

        assert _get_autocommit() is True

        with client.sql_client.begin_transaction():
            assert _get_autocommit() is False

        assert _get_autocommit() is True


def test_open_connection_params(client: SnowflakeClient, mocker: MockerFixture) -> None:
    client.sql_client.close_connection()
    connect_spy = mocker.patch("dlt.destinations.impl.snowflake.sql_client.snowflake_lib.connect")
    client.sql_client.open_connection()
    kwargs = connect_spy.call_args.kwargs
    assert kwargs["schema"] == client.sql_client.fully_qualified_dataset_name()
    assert kwargs["autocommit"] is True
    assert kwargs["arrow_number_to_decimal"] is True
    # connector 5.x warns when logout behavior is not set explicitly
    assert kwargs["enable_server_session_keep_alive_auto_detection"] is False

    # query params override the defaults
    client.sql_client.credentials.query = {"enable_server_session_keep_alive_auto_detection": True}
    client.sql_client.open_connection()
    assert connect_spy.call_args.kwargs["enable_server_session_keep_alive_auto_detection"] is True


def test_load_with_connection_name(snowflake_home: Callable[[str, str], Path]) -> None:
    """Loads through a connection in `connections.toml` built from the test credentials."""
    from cryptography.hazmat.primitives import serialization

    creds = resolve_configuration(
        SnowflakeCredentials(), sections=("destination", "snowflake", "credentials")
    )
    profile = [
        f'account = "{creds.host}"',
        f'user = "{creds.username}"',
        f'database = "{creds.database}"',
        "client_session_keep_alive = true",
    ]
    for param in ("warehouse", "role"):
        if getattr(creds, param):
            profile.append(f'{param} = "{getattr(creds, param)}"')
    if creds.private_key:
        # the connector loads PEM key files, dlt keys may be base64 DER
        der = decode_private_key(creds.private_key, creds.private_key_passphrase)
        pem = serialization.load_der_private_key(der, None).private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        key_path = snowflake_home("", "") / "ci_key.pem"
        key_path.write_bytes(pem)
        os.chmod(key_path, 0o600)
        profile += [
            'authenticator = "SNOWFLAKE_JWT"',
            f'private_key_file = "{key_path.as_posix()}"',
        ]
    elif creds.password:
        profile.append(f'password = "{creds.password}"')
    else:
        pytest.skip("test credentials use an authentication the profile does not cover")
    snowflake_home("[ci]\n" + "\n".join(profile), "")

    # no sections: the `destination.snowflake` secrets stay out of these credentials
    conn_creds = resolve_configuration(
        SnowflakeCredentials(), explicit_value={"connection_name": "ci"}
    )
    conn_params = conn_creds.to_connector_params()
    assert conn_params["connection_name"] == "ci"
    # the connector loads the credentials, dlt sends only session settings
    assert not conn_params.keys() & {"user", "password", "account", "private_key", "token"}

    pipeline = dlt.pipeline(
        "sf_conn_" + uniq_id(),
        destination=dlt.destinations.snowflake(credentials=conn_creds),
        dataset_name="ds_" + uniq_id(),
        dev_mode=True,
    )
    info = pipeline.run([{"id": 1, "name": "a"}], table_name="items")
    assert_load_info(info)
    with pipeline.destination_client() as client:
        assert client.config.data_location() == creds.host
        assert client.config.fingerprint() == digest128(creds.host)
        native_conn = client.sql_client.native_connection  # type: ignore[attr-defined]
        assert native_conn.client_session_keep_alive is True
        assert native_conn.database.upper() == creds.database.upper()
