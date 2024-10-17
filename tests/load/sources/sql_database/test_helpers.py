import pytest


import dlt
from dlt.common.typing import TDataItem


from dlt.common.exceptions import MissingDependencyException

try:
    from dlt.sources.sql_database.helpers import TableLoader, TableBackend
    from dlt.sources.sql_database.schema_types import table_to_columns
    from tests.load.sources.sql_database.sql_source import SQLAlchemySourceDB
    import sqlalchemy as sa
except (MissingDependencyException, ModuleNotFoundError):
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
@pytest.mark.parametrize("cursor_value_missing", ["include", "exclude"])
def test_make_query_incremental_on_cursor_value_missing_set(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
    with_end_value: bool,
    cursor_value_missing: str,
) -> None:
    class MockIncremental:
        last_value = dlt.common.pendulum.now()
        last_value_func = max
        cursor_path = "created_at"
        row_order = "asc"
        end_value = None if not with_end_value else dlt.common.pendulum.now().add(hours=1)
        on_cursor_value_missing = cursor_value_missing

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=MockIncremental(),  # type: ignore[arg-type]
    )

    query = loader.make_query()
    if cursor_value_missing == "include":
        missing_cond = table.c.created_at.is_(None)
        operator = sa.or_
    else:
        missing_cond = table.c.created_at.isnot(None)
        operator = sa.and_

    if with_end_value:
        where_clause = operator(
            sa.and_(
                table.c.created_at >= MockIncremental.last_value,
                table.c.created_at < MockIncremental.end_value,
            ),
            missing_cond,
        )
    else:
        where_clause = operator(
            table.c.created_at >= MockIncremental.last_value,
            missing_cond,
        )
    expected = table.select().order_by(table.c.created_at.asc()).where(where_clause)
    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("cursor_value_missing", ["include", "exclude"])
def test_make_query_incremental_on_cursor_value_missing_no_last_value(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
    cursor_value_missing: str,
) -> None:
    class MockIncremental:
        last_value = None
        last_value_func = max
        cursor_path = "created_at"
        row_order = "asc"
        end_value = None
        on_cursor_value_missing = cursor_value_missing

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=MockIncremental(),  # type: ignore[arg-type]
    )

    query = loader.make_query()

    if cursor_value_missing == "include":
        # There is no where clause for include without last
        expected = table.select().order_by(table.c.created_at.asc())
    else:
        # exclude always has a where clause
        expected = (
            table.select().order_by(table.c.created_at.asc()).where(table.c.created_at.isnot(None))
        )

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
