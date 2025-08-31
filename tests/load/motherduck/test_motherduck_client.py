import os
from typing import Any, List, Optional
import pytest
from pytest_mock import MockerFixture

from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.resolve import resolve_configuration

from dlt.destinations.impl.motherduck.configuration import (
    MOTHERDUCK_USER_AGENT,
    MotherDuckCredentials,
    MotherDuckClientConfiguration,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_motherduck_configuration() -> None:
    cred = MotherDuckCredentials("md:///dlt_data?token=TOKEN")
    # print(dict(cred))
    assert cred.password == "TOKEN"
    assert cred.database == "dlt_data"
    assert cred.is_partial() is False
    assert cred.is_resolved() is False

    cred = MotherDuckCredentials()
    cred.parse_native_representation("md:///?motherduck_token=TOKEN")
    assert cred.password == "TOKEN"
    assert cred.database == ""
    assert cred.is_partial() is False
    assert cred.is_resolved() is False

    cred = MotherDuckCredentials()
    cred.parse_native_representation("md:xdb?motherduck_token=TOKEN2")
    assert cred.password == "TOKEN2"
    assert cred.database == "xdb"
    assert cred.is_partial() is False
    assert cred.is_resolved() is False

    # password or token are mandatory
    with pytest.raises(ConfigFieldMissingException) as conf_ex:
        resolve_configuration(MotherDuckCredentials())
    assert conf_ex.value.fields == ["password"]

    os.environ["CREDENTIALS__PASSWORD"] = "pwd"
    config = resolve_configuration(MotherDuckCredentials())
    assert config.password == "pwd"

    del os.environ["CREDENTIALS__PASSWORD"]
    os.environ["CREDENTIALS__QUERY"] = '{"token": "tok"}'
    config = resolve_configuration(MotherDuckCredentials())
    assert config.password == "tok"


@pytest.mark.parametrize("token_key", ["motherduck_token", "MOTHERDUCK_TOKEN"])
def test_motherduck_connect_default_token(token_key: str) -> None:
    import dlt

    credentials = dlt.secrets.get(
        "destination.motherduck.credentials", expected_type=MotherDuckCredentials
    )
    assert credentials.password
    os.environ[token_key] = credentials.password

    credentials = MotherDuckCredentials()
    assert credentials._has_default_token() is True
    credentials.on_partial()
    assert credentials.is_resolved()

    config = MotherDuckClientConfiguration(credentials=credentials)
    print(config.credentials._conn_str())
    # connect
    con = config.credentials.conn_pool.borrow_conn()
    con.sql("SHOW DATABASES")
    config.credentials.conn_pool.return_conn(con)


def test_credentials_wrong_config() -> None:
    import duckdb

    c = resolve_configuration(
        MotherDuckClientConfiguration()._bind_dataset_name(dataset_name="test_dataset"),
        sections=("destination", "motherduck"),
    )
    with pytest.raises(duckdb.CatalogException):
        c.credentials.conn_pool.borrow_conn(global_config={"wrong_conf": 0})
    # connection closed
    assert c.credentials.conn_pool._conn is None
    assert c.credentials.conn_pool._conn_borrows == 0

    with pytest.raises(duckdb.CatalogException):
        c.credentials.conn_pool.borrow_conn(local_config={"wrong_conf": 0})
    # connection closed
    assert c.credentials.conn_pool._conn is None
    assert c.credentials.conn_pool._conn_borrows == 0

    with pytest.raises(duckdb.CatalogException):
        c.credentials.conn_pool.borrow_conn(pragmas=["unkn_pragma"])
    # connection closed
    assert c.credentials.conn_pool._conn is None
    assert c.credentials.conn_pool._conn_borrows == 0

    # open and borrow conn
    conn = c.credentials.conn_pool.borrow_conn()
    assert c.credentials.conn_pool._conn_borrows == 1
    try:
        with pytest.raises(duckdb.CatalogException):
            c.credentials.conn_pool.borrow_conn(pragmas=["unkn_pragma"])
        assert c.credentials.conn_pool._conn is not None
        # refcount not increased
        assert c.credentials.conn_pool._conn_borrows == 1
    finally:
        conn.close()

    # check extensions fail
    os.environ["CREDENTIALS__EXTENSIONS"] = '["unk_extension"]'
    c = resolve_configuration(
        MotherDuckClientConfiguration()._bind_dataset_name(dataset_name="test_dataset"),
        sections=("destination", "motherduck"),
    )
    with pytest.raises(duckdb.IOException):
        c.credentials.conn_pool.borrow_conn()
    assert not hasattr(c.credentials, "_conn")
    assert c.credentials.conn_pool._conn_borrows == 0


@pytest.mark.parametrize("custom_user_agent", [MOTHERDUCK_USER_AGENT, "patates", None])
def test_motherduck_connect_with_user_agent_string(custom_user_agent: Optional[str]) -> None:
    import duckdb

    config = resolve_configuration(
        MotherDuckClientConfiguration()._bind_dataset_name(dataset_name="test"),
        sections=("destination", "motherduck"),
        # TODO: change resolver to allow this
        # explicit_value={
        #     "credentials": {
        #         "global_config": {
        #             "custom_user_agent": custom_user_agent
        #         },
        #         "local_config": {
        #             "enable_logging": True
        #         },
        #         "query": {
        #             "motherduck_dbinstance_inactivity_ttl": "0s"
        #         },
        #     }
        # }
    )
    config.credentials.update(
        {
            "custom_user_agent": custom_user_agent,
            # "local_config": {"enable_logging": True},
            "query": {"dbinstance_inactivity_ttl": "0s"},
            "local_config": {"enable_profiling": "json"},
        }
    )
    config.credentials.resolve()
    assert config.credentials.custom_user_agent == custom_user_agent
    # query params will be passed to conn str, motherduck_token added automatically
    assert config.credentials._conn_str().startswith(
        "md:dlt_data?dbinstance_inactivity_ttl=0s&motherduck_token="
    )

    def _read_config(conn: duckdb.DuckDBPyConnection) -> List[Any]:
        rel = conn.sql("""
            SELECT name, value
            FROM duckdb_settings()
            WHERE name IN ('enable_profiling', 'custom_user_agent', 'motherduck_dbinstance_inactivity_ttl')
            ORDER BY name ASC;
            """)
        return rel.fetchall()

    # connect
    con = config.credentials.conn_pool.borrow_conn()
    try:
        assert _read_config(con) == [
            ("custom_user_agent", custom_user_agent or MOTHERDUCK_USER_AGENT),
            ("enable_profiling", "json"),
            ("motherduck_dbinstance_inactivity_ttl", "0s"),
        ]
    finally:
        config.credentials.conn_pool.return_conn(con)
