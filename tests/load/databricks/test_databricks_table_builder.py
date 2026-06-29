from tests.utils import skip_if_not_active

skip_if_not_active("databricks")

from typing import List
from unittest import mock

import pytest

from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema
from dlt.common.schema.utils import new_table
from dlt.common.utils import uniq_id
from dlt.destinations import databricks
from dlt.destinations.exceptions import DatabaseTerminalException
from dlt.destinations.impl.databricks.databricks import DatabricksClient
from dlt.destinations.impl.databricks.configuration import (
    DatabricksClientConfiguration,
    DatabricksCredentials,
)

from tests.load.utils import empty_schema

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def create_client(schema: Schema, create_indexes: bool) -> DatabricksClient:
    # return a client without opening connection
    creds = DatabricksCredentials()
    creds.catalog = "test_catalog"
    creds.server_hostname = "test.databricks.com"
    creds.http_path = "/sql/1.0/endpoints/test"
    creds.access_token = "test-token"
    config = DatabricksClientConfiguration(
        credentials=creds, create_indexes=create_indexes
    )._bind_dataset_name(dataset_name="test_" + uniq_id())
    return databricks().client(schema, config)


@pytest.mark.parametrize("create_indexes", [True, False], ids=["indexes", "no_indexes"])
def test_foreign_key_constraint_conditional_on_create_indexes(
    empty_schema: Schema, create_indexes: bool
) -> None:
    table = new_table(
        "user_sessions",
        columns=[{"name": "user_id", "data_type": "bigint"}],
        references=[
            {
                "referenced_table": "users",
                "columns": ["user_id"],
                "referenced_columns": ["id"],
            }
        ],
    )
    client = create_client(empty_schema, create_indexes=create_indexes)

    sql = client._get_table_post_update_sql(table)

    if create_indexes:
        assert len(sql) == 1
        stmt = sql[0]
        assert "ADD FOREIGN KEY" in stmt
        assert "`user_id`" in stmt
        assert "REFERENCES" in stmt
        assert "`users`" in stmt
        assert "`id`" in stmt
    else:
        assert sql == []


@pytest.mark.parametrize("create_indexes", [True, False], ids=["indexes", "no_indexes"])
def test_primary_key_constraint_conditional_on_create_indexes(
    empty_schema: Schema, create_indexes: bool
) -> None:
    columns: List[TColumnSchema] = [
        {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
        {"name": "value", "data_type": "text"},
    ]
    client = create_client(empty_schema, create_indexes=create_indexes)

    sql = client._get_table_update_sql("event_test_table", columns, generate_alter=False)[0]

    if create_indexes:
        assert "PRIMARY KEY (`id`)" in sql
    else:
        assert "PRIMARY KEY" not in sql


@pytest.mark.parametrize("with_cluster", [False, True], ids=["plain", "clustered"])
def test_create_table_uses_if_not_exists(empty_schema: Schema, with_cluster: bool) -> None:
    columns: List[TColumnSchema] = [
        {"name": "col_a", "data_type": "text", "cluster": with_cluster},
        {"name": "col_b", "data_type": "bigint"},
    ]
    client = create_client(empty_schema, create_indexes=False)

    # generate_alter=False takes both the base and the custom-clause CREATE paths
    sql = client._get_table_update_sql("event_test_table", columns, generate_alter=False)[0]

    assert sql.startswith("CREATE TABLE IF NOT EXISTS")


def test_reconcile_adds_columns_missed_by_concurrent_create(empty_schema: Schema) -> None:
    table_name = "event_test_table"
    columns: List[TColumnSchema] = [
        {"name": "col_a", "data_type": "text"},
        {"name": "col_b", "data_type": "bigint"},
        {"name": "col_c", "data_type": "bool"},
    ]
    client = create_client(empty_schema, create_indexes=False)
    client.schema.update_table(new_table(table_name, columns=columns))

    # a concurrent load won the create race with only col_a and col_b
    storage_columns = {c["name"]: c for c in columns[:2]}
    client.get_storage_tables = mock.MagicMock(return_value=[(table_name, storage_columns)])  # type: ignore[method-assign]
    executed: List[str] = []
    client.sql_client.execute_sql = mock.MagicMock(  # type: ignore[method-assign]
        side_effect=lambda sql, *args: executed.append(sql)
    )

    client._reconcile_columns_after_create([table_name])

    assert any("ADD COLUMN" in sql and "`col_c`" in sql for sql in executed)
    assert not any("`col_a`" in sql for sql in executed)


def test_reconcile_tolerates_column_added_by_concurrent_load(empty_schema: Schema) -> None:
    table_name = "event_test_table"
    columns: List[TColumnSchema] = [
        {"name": "col_a", "data_type": "text"},
        {"name": "col_b", "data_type": "bigint"},
    ]
    client = create_client(empty_schema, create_indexes=False)
    client.schema.update_table(new_table(table_name, columns=columns))
    client.get_storage_tables = mock.MagicMock(  # type: ignore[method-assign]
        return_value=[(table_name, {"col_a": columns[0]})]
    )

    fields_exist = DatabaseTerminalException(Exception("[FIELDS_ALREADY_EXISTS] col_b"))
    client.sql_client.execute_sql = mock.MagicMock(side_effect=fields_exist)  # type: ignore[method-assign]
    # the concurrent column is swallowed
    client._reconcile_columns_after_create([table_name])

    other_error = DatabaseTerminalException(Exception("PARSE_SYNTAX_ERROR"))
    client.sql_client.execute_sql = mock.MagicMock(side_effect=other_error)  # type: ignore[method-assign]
    with pytest.raises(DatabaseTerminalException):
        client._reconcile_columns_after_create([table_name])
