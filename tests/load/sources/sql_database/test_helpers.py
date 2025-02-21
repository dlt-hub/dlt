from functools import partial
from typing import Callable, Any, Literal
from dataclasses import dataclass

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


@dataclass
class MockIncremental:
    last_value: Any
    last_value_func: Callable[[Any], Any]
    cursor_path: str
    row_order: str = None
    end_value: Any = None
    on_cursor_value_missing: str = "raise"
    range_start: str = "closed"
    range_end: str = "open"


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

    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=max,
        cursor_path="created_at",
        row_order="asc",
    )

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=incremental,  # type: ignore[arg-type]
    )

    query = loader.make_query()
    expected = (
        table.select()
        .order_by(table.c.created_at.asc())
        .where(table.c.created_at >= incremental.last_value)
    )

    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_incremental_min(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=min,
        cursor_path="created_at",
        row_order="desc",
        end_value=None,
        on_cursor_value_missing="raise",
    )

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=incremental,  # type: ignore[arg-type]
    )

    query = loader.make_query()
    expected = (
        table.select()
        .order_by(table.c.created_at.asc())  # `min` func swaps order
        .where(table.c.created_at <= incremental.last_value)
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
    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=max,
        cursor_path="created_at",
        row_order="asc",
        end_value=None if not with_end_value else dlt.common.pendulum.now().add(hours=1),
        on_cursor_value_missing=cursor_value_missing,
    )

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=incremental,  # type: ignore[arg-type]
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
                table.c.created_at >= incremental.last_value,
                table.c.created_at < incremental.end_value,
            ),
            missing_cond,
        )
    else:
        where_clause = operator(
            table.c.created_at >= incremental.last_value,
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
    incremental = MockIncremental(
        last_value=None,
        last_value_func=max,
        cursor_path="created_at",
        row_order="asc",
        end_value=None,
        on_cursor_value_missing=cursor_value_missing,
    )

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=incremental,  # type: ignore[arg-type]
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

    incremental = MockIncremental(
        last_value=now,
        last_value_func=min,
        cursor_path="created_at",
        end_value=now.add(hours=1),
        row_order=None,
        on_cursor_value_missing="raise",
    )

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=incremental,  # type: ignore[arg-type]
    )

    query = loader.make_query()
    expected = table.select().where(
        sa.and_(
            table.c.created_at <= incremental.last_value,
            table.c.created_at > incremental.end_value,
        )
    )

    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_incremental_any_fun(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=lambda x: x[-1],
        cursor_path="created_at",
        row_order="asc",
        end_value=dlt.common.pendulum.now(),
        on_cursor_value_missing="raise",
    )

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=incremental,  # type: ignore[arg-type]
    )

    query = loader.make_query()
    expected = table.select()

    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_cursor_path_field_name_with_a_special_chars(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    """Test that a field name with special characters in cursor_path is handled correctly."""
    table = sql_source_db.get_table("chat_message")

    # Add a mock column with a special character
    special_field_name = "id$field"
    if special_field_name not in table.c:
        table.append_column(sa.Column(special_field_name, sa.String))

    incremental = MockIncremental(
        cursor_path="'id$field'",
        last_value=None,
        last_value_func=max,
    )

    # Should not raise any exception
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=incremental,  # type: ignore[arg-type]
    )
    assert loader.cursor_column == table.c[special_field_name]


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_cursor_path_multiple_fields(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    """Test that a cursor_path with multiple fields raises a ValueError."""
    table = sql_source_db.get_table("chat_message")

    incremental = MockIncremental(
        cursor_path="created_at,updated_at",
        last_value=None,
        last_value_func=max,
    )

    with pytest.raises(ValueError) as excinfo:
        TableLoader(
            sql_source_db.engine,
            backend,
            table,
            table_to_columns(table),
            incremental=incremental,  # type: ignore[arg-type]
        )
    assert "must be a simple column name" in str(excinfo.value)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_cursor_path_complex_expression(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    """Test that a complex JSONPath expression in cursor_path raises a ValueError."""
    table = sql_source_db.get_table("chat_message")

    incremental = MockIncremental(
        cursor_path="$.users[0].id",
        last_value=None,
        last_value_func=max,
    )

    with pytest.raises(ValueError) as excinfo:
        TableLoader(
            sql_source_db.engine,
            backend,
            table,
            table_to_columns(table),
            incremental=incremental,  # type: ignore[arg-type]
        )
    assert "must be a simple column name" in str(excinfo.value)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("last_value_func", [min, max])
def test_make_query_incremental_range_start_open(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend, last_value_func: Callable[[Any], Any]
) -> None:
    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=last_value_func,
        cursor_path="created_at",
        end_value=None,
        on_cursor_value_missing="raise",
        range_start="open",
    )

    table = sql_source_db.get_table("chat_message")

    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=incremental,  # type: ignore[arg-type]
    )

    query = loader.make_query()
    expected = table.select()

    if last_value_func == min:
        expected = expected.where(table.c.created_at < incremental.last_value)
    else:
        expected = expected.where(table.c.created_at > incremental.last_value)

    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("last_value_func", [min, max])
def test_make_query_incremental_range_end_closed(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend, last_value_func: Callable[[Any], Any]
) -> None:
    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=last_value_func,
        cursor_path="created_at",
        end_value=None,
        on_cursor_value_missing="raise",
        range_end="closed",
    )

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=incremental,  # type: ignore[arg-type]
    )

    query = loader.make_query()
    expected = table.select()

    if last_value_func == min:
        expected = expected.where(table.c.created_at <= incremental.last_value)
    else:
        expected = expected.where(table.c.created_at >= incremental.last_value)

    assert query.compare(expected)


def mock_column(field: str, mock_type: Literal["json", "array"] = "json") -> TDataItem:
    """"""
    import pyarrow as pa
    import pandas as pd

    if mock_type == "json":
        mock_str = '{"data": [1, 2, 3]}'
    elif mock_type == "array":
        mock_str = "[1, 2, 3]"

    def _unwrap(table: TDataItem) -> TDataItem:
        if isinstance(table, pd.DataFrame):
            table[field] = [None if s is None else mock_str for s in table[field]]
            return table
        else:
            col_index = table.column_names.index(field)
            json_str_array = pa.array([None if s is None else mock_str for s in table[field]])
            return table.set_column(
                col_index,
                pa.field(field, pa.string(), nullable=table.schema.field(field).nullable),
                json_str_array,
            )

    return _unwrap


mock_json_column = partial(mock_column, mock_type="json")
mock_array_column = partial(mock_column, mock_type="array")
