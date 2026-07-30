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


def create_client(
    schema: Schema, create_indexes: bool, create_comments: bool = True
) -> DatabricksClient:
    # return a client without opening connection
    creds = DatabricksCredentials()
    creds.catalog = "test_catalog"
    creds.server_hostname = "test.databricks.com"
    creds.http_path = "/sql/1.0/endpoints/test"
    creds.access_token = "test-token"
    config = DatabricksClientConfiguration(
        credentials=creds, create_indexes=create_indexes, create_comments=create_comments
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


@pytest.mark.parametrize("create_comments", [True, False], ids=["comments", "no_comments"])
def test_comments_enabled_by_create_comments_config(
    empty_schema: Schema, create_comments: bool
) -> None:
    columns: List[TColumnSchema] = [
        {"name": "col_a", "data_type": "text", "description": "a column"}
    ]
    client = create_client(empty_schema, create_indexes=False, create_comments=create_comments)
    client.schema.update_table(new_table("event_test_table", columns=columns))
    client.schema.tables["event_test_table"]["description"] = "a table"

    sql = "\n".join(client._get_table_update_sql("event_test_table", columns, generate_alter=False))

    if create_comments:
        assert "COMMENT ON TABLE" in sql
        assert "COMMENT 'a column'" in sql
    else:
        assert "COMMENT" not in sql


def test_no_comments_on_dlt_system_tables(empty_schema: Schema) -> None:
    # dlt system tables carry a "Created by DLT..." description that must not be emitted
    client = create_client(empty_schema, create_indexes=False)
    loads_table = client.schema.loads_table_name
    assert client.schema.tables[loads_table].get("description")
    columns = list(client.schema.tables[loads_table]["columns"].values())

    sql = "\n".join(client._get_table_update_sql(loads_table, columns, generate_alter=False))

    assert "COMMENT" not in sql
