import pytest

import sqlalchemy as sa

import dlt
from dlt.common.typing import TDataItem


from dlt.common.exceptions import MissingDependencyException

try:
    from dlt.sources.sql_database.helpers import TableLoader, TableBackend
    from dlt.sources.sql_database.schema_types import table_to_columns
    from tests.load.sources.sql_database.sql_source import SQLAlchemySourceDB
except MissingDependencyException:
    pytest.skip("Tests require sql alchemy", allow_module_level=True)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_cursor_or_unique_column_not_in_table(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    table = sql_source_db.get_table("chat_message")

    with pytest.raises(KeyError):
        TableLoader(
            sql_source_db.engine,
            backend,
            table,
            table_to_columns(table),
            incremental=dlt.sources.incremental("not_a_column"),
        )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_incremental_max(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    """Verify query is generated according to incremental settings"""

    class MockIncremental:
        last_value = dlt.common.pendulum.now()
        last_value_func = max
        cursor_path = "created_at"
        row_order = "asc"
        end_value = None
        on_cursor_value_missing = "raise"

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=MockIncremental(),  # type: ignore[arg-type]
    )

    query = loader.make_query()
    expected = (
        table.select()
        .order_by(table.c.created_at.asc())
        .where(table.c.created_at >= MockIncremental.last_value)
    )

    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_incremental_min(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    class MockIncremental:
        last_value = dlt.common.pendulum.now()
        last_value_func = min
        cursor_path = "created_at"
        row_order = "desc"
        end_value = None
        on_cursor_value_missing = "raise"

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=MockIncremental(),  # type: ignore[arg-type]
    )

    query = loader.make_query()
    expected = (
        table.select()
        .order_by(table.c.created_at.asc())  # `min` func swaps order
        .where(table.c.created_at <= MockIncremental.last_value)
    )

    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("with_end_value", [True, False])
def test_make_query_incremental_include_none(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend, with_end_value: bool
) -> None:
    class MockIncremental:
        last_value = dlt.common.pendulum.now()
        last_value_func = max
        cursor_path = "created_at"
        row_order = "asc"
        end_value = None if not with_end_value else dlt.common.pendulum.now().add(hours=1)
        on_cursor_value_missing = "include"

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=MockIncremental(),  # type: ignore[arg-type]
    )

    query = loader.make_query()
    if with_end_value:
        where_clause = sa.or_(
            sa.and_(
                table.c.created_at >= MockIncremental.last_value,
                table.c.created_at < MockIncremental.end_value,
            ),
            table.c.created_at.is_(None),
        )
    else:
        where_clause = sa.or_(
            table.c.created_at >= MockIncremental.last_value,
            table.c.created_at.is_(None),
        )
    expected = table.select().order_by(table.c.created_at.asc()).where(where_clause)
    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_incremental_end_value(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    now = dlt.common.pendulum.now()

    class MockIncremental:
        last_value = now
        last_value_func = min
        cursor_path = "created_at"
        end_value = now.add(hours=1)
        row_order = None
        on_cursor_value_missing = "raise"

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=MockIncremental(),  # type: ignore[arg-type]
    )

    query = loader.make_query()
    expected = table.select().where(
        sa.and_(
            table.c.created_at <= MockIncremental.last_value,
            table.c.created_at > MockIncremental.end_value,
        )
    )

    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_incremental_any_fun(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    class MockIncremental:
        last_value = dlt.common.pendulum.now()
        last_value_func = lambda x: x[-1]
        cursor_path = "created_at"
        row_order = "asc"
        end_value = dlt.common.pendulum.now()
        on_cursor_value_missing = "raise"

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=MockIncremental(),  # type: ignore[arg-type]
    )

    query = loader.make_query()
    expected = table.select()

    assert query.compare(expected)


def mock_json_column(field: str) -> TDataItem:
    """"""
    import pyarrow as pa
    import pandas as pd

    json_mock_str = '{"data": [1, 2, 3]}'

    def _unwrap(table: TDataItem) -> TDataItem:
        if isinstance(table, pd.DataFrame):
            table[field] = [None if s is None else json_mock_str for s in table[field]]
            return table
        else:
            col_index = table.column_names.index(field)
            json_str_array = pa.array([None if s is None else json_mock_str for s in table[field]])
            return table.set_column(
                col_index,
                pa.field(field, pa.string(), nullable=table.schema.field(field).nullable),
                json_str_array,
            )

    return _unwrap
