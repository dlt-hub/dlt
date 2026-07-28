from tests.utils import skip_if_not_active

skip_if_not_active("databricks")

from typing import List

import pytest

from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema
from dlt.common.schema.utils import new_table
from dlt.common.utils import uniq_id
from dlt.destinations import databricks
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
    # generate_alter=False takes both the base and the custom-clause CREATE paths
    columns: List[TColumnSchema] = [
        {"name": "col_a", "data_type": "text", "cluster": with_cluster},
        {"name": "col_b", "data_type": "bigint"},
    ]
    client = create_client(empty_schema, create_indexes=False)

    sql = client._get_table_update_sql("event_test_table", columns, generate_alter=False)[0]

    assert sql.startswith("CREATE TABLE IF NOT EXISTS")


@pytest.mark.parametrize("with_cluster", [False, True], ids=["plain", "clustered"])
def test_table_comment_generation(empty_schema: Schema, with_cluster: bool) -> None:
    table_name = "event_test_table"
    columns: List[TColumnSchema] = [{"name": "col_a", "data_type": "text", "cluster": with_cluster}]
    client = create_client(empty_schema, create_indexes=False)
    table = new_table(table_name, columns=columns)
    table["description"] = "test table comment"
    client.schema.update_table(table)

    create_sql = client._get_table_update_sql(table_name, columns, generate_alter=False)
    assert len(create_sql) == 1
    assert "COMMENT 'test table comment'" in create_sql[0]
    assert "COMMENT ON TABLE" not in create_sql[0]

    alter_sql = client._get_table_update_sql(table_name, columns, generate_alter=True)
    assert any(s.startswith("COMMENT ON TABLE") for s in alter_sql)
