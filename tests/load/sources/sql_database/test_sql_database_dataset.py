import pytest

import pyarrow as pa

from dlt.common.exceptions import MissingDependencyException

try:
    from dlt.sources.sql_database import (
        TableBackend,
        sql_database,
        sql_table,
    )
    from dlt.sources.sql_database.dataset import sql_database_dataset
    from tests.load.sources.sql_database.postgres_source import PostgresSourceDB
except MissingDependencyException:
    pytest.skip("Tests require sql alchemy", allow_module_level=True)


BACKENDS = ["sqlalchemy", "pyarrow", "pandas", "connectorx"]


@pytest.mark.parametrize("backend", BACKENDS)
def test_sql_database_dataset_read(postgres_db: PostgresSourceDB, backend: TableBackend) -> None:
    source = sql_database(
        credentials=postgres_db.credentials,
        schema=postgres_db.schema,
        table_names=["app_user", "chat_message", "camelCaseTable"],
        backend=backend,
    )
    dataset = sql_database_dataset(source)
    assert dataset.dataset_name == postgres_db.schema
    assert {"app_user", "chat_message", "camelCaseTable"}.issubset(dataset.tables)

    # full results in all formats
    row_count = postgres_db.table_infos["chat_message"]["row_count"]
    assert dataset.table("chat_message").arrow().num_rows == row_count
    assert len(dataset.table("chat_message").df()) == row_count
    assert len(dataset.table("chat_message").fetchall()) == row_count
    assert len(dataset.table("chat_message").fetchmany(5)) == 5
    assert dataset.table("chat_message").fetchone() is not None

    # chunked results, connectorx returns a single table without streaming
    chunks = list(dataset.table("chat_message").iter_arrow(chunk_size=7))
    assert sum(chunk.num_rows for chunk in chunks) == row_count
    if backend != "connectorx":
        assert max(chunk.num_rows for chunk in chunks) == 7

    # database identifiers are preserved
    camel = dataset.table("camelCaseTable").select("firstName", "isActive").arrow()
    assert camel.column_names == ["firstName", "isActive"]
    assert camel.num_rows == 100

    # sqlglot query with a join and a filter
    joined = dataset(
        "SELECT u.id, count(*) AS messages FROM chat_message m JOIN app_user u ON"
        " m.user_id = u.id GROUP BY u.id"
    ).fetchall()
    assert sum(row[1] for row in joined) == row_count

    # empty result keeps the columns
    empty = dataset.table("app_user").where("id", "lt", 0).arrow()
    assert empty.num_rows == 0
    assert "email" in empty.column_names


@pytest.mark.parametrize("backend", ["pyarrow", "connectorx"])
def test_sql_database_dataset_types_match_extract(
    postgres_db: PostgresSourceDB, backend: TableBackend
) -> None:
    resource = sql_table(
        credentials=postgres_db.credentials,
        schema=postgres_db.schema,
        table="has_precision",
        backend=backend,
        reflection_level="full_with_precision",
    )
    extracted = pa.concat_tables(list(resource))
    dataset_table = sql_database_dataset(resource).table("has_precision").arrow()
    assert dataset_table.schema.remove_metadata() == extracted.schema.remove_metadata()


def test_sql_database_dataset_with_resources(postgres_db: PostgresSourceDB) -> None:
    source = sql_database(
        credentials=postgres_db.credentials,
        schema=postgres_db.schema,
        backend="pyarrow",
    )
    dataset = sql_database_dataset(source.with_resources("app_user"))
    assert dataset.tables[0] == "app_user"
    assert "chat_message" not in dataset.tables
    assert dataset.table("app_user").arrow().num_rows == (
        postgres_db.table_infos["app_user"]["row_count"]
    )
