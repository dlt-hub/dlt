import os
from functools import partial
from typing import Callable, Any, Dict, Iterator, List, Literal
from dataclasses import dataclass

import pytest

import dlt
from dlt.common.typing import TDataItem

from dlt.common.exceptions import MissingDependencyException
from dlt.extract.items_transform import LimitItem

try:
    from dlt.sources.sql_database.helpers import (
        BaseTableLoader,
        TableLoader,
        ConnectorXTableLoader,
        TableBackend,
        TABLE_LOADER_REGISTRY,
        register_table_loader_backend,
        get_table_loader_class,
    )
    from dlt.sources.sql_database.schema_types import table_to_columns
    from tests.load.sources.sql_database.postgres_source import PostgresSourceDB
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
    postgres_db: PostgresSourceDB, backend: TableBackend
) -> None:
    table = postgres_db.get_table("chat_message")

    with pytest.raises(KeyError):
        TableLoader(
            postgres_db.engine,
            backend,
            table,
            table_to_columns(table),
            incremental=dlt.sources.incremental("not_a_column"),
        )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_incremental_max(postgres_db: PostgresSourceDB, backend: TableBackend) -> None:
    """Verify query is generated according to incremental settings"""

    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=max,
        cursor_path="created_at",
        row_order="asc",
    )

    table = postgres_db.get_table("chat_message")
    loader = TableLoader(
        postgres_db.engine,
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
def test_make_query_incremental_min(postgres_db: PostgresSourceDB, backend: TableBackend) -> None:
    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=min,
        cursor_path="created_at",
        row_order="desc",
        end_value=None,
        on_cursor_value_missing="raise",
    )

    table = postgres_db.get_table("chat_message")
    loader = TableLoader(
        postgres_db.engine,
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
    postgres_db: PostgresSourceDB,
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

    table = postgres_db.get_table("chat_message")
    loader = TableLoader(
        postgres_db.engine,
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
    postgres_db: PostgresSourceDB,
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

    table = postgres_db.get_table("chat_message")
    loader = TableLoader(
        postgres_db.engine,
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
    postgres_db: PostgresSourceDB, backend: TableBackend
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

    table = postgres_db.get_table("chat_message")
    loader = TableLoader(
        postgres_db.engine,
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
def test_make_query_incremental_end_value_and_limit(
    postgres_db: PostgresSourceDB, backend: TableBackend
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

    table = postgres_db.get_table("chat_message")
    loader = TableLoader(
        postgres_db.engine,
        backend,
        table,
        table_to_columns(table),
        chunk_size=1000,
        limit=LimitItem(10, None, False),
        incremental=incremental,  # type: ignore[arg-type]
    )

    query = loader.make_query()
    expected = (
        table.select()
        .limit(10000)
        .where(
            sa.and_(
                table.c.created_at <= incremental.last_value,
                table.c.created_at > incremental.end_value,
            )
        )
    )

    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_with_limit(postgres_db: PostgresSourceDB, backend: TableBackend) -> None:
    table = postgres_db.get_table("chat_message")
    loader = TableLoader(
        postgres_db.engine,
        backend,
        table,
        table_to_columns(table),
        chunk_size=1000,
        limit=LimitItem(10, None, False),
    )

    query = loader.make_query()
    expected = table.select().limit(10000)
    assert query.compare(expected)

    loader = TableLoader(
        postgres_db.engine,
        backend,
        table,
        table_to_columns(table),
        chunk_size=1000,
        limit=LimitItem(None, max_time=10.0, count_rows=False),
    )

    query = loader.make_query()
    # limit will not be added
    expected = table.select()
    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_incremental_any_fun(
    postgres_db: PostgresSourceDB, backend: TableBackend
) -> None:
    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=lambda x: x[-1],
        cursor_path="created_at",
        row_order="asc",
        end_value=dlt.common.pendulum.now(),
        on_cursor_value_missing="raise",
    )

    table = postgres_db.get_table("chat_message")
    loader = TableLoader(
        postgres_db.engine,
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
    postgres_db: PostgresSourceDB, backend: TableBackend
) -> None:
    """Test that a field name with special characters in cursor_path is handled correctly."""
    table = postgres_db.get_table("chat_message")

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
        postgres_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=incremental,  # type: ignore[arg-type]
    )
    assert loader.cursor_column == table.c[special_field_name]


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_cursor_path_multiple_fields(postgres_db: PostgresSourceDB, backend: TableBackend) -> None:
    """Test that a cursor_path with multiple fields raises a ValueError."""
    table = postgres_db.get_table("chat_message")

    incremental = MockIncremental(
        cursor_path="created_at,updated_at",
        last_value=None,
        last_value_func=max,
    )

    with pytest.raises(ValueError) as excinfo:
        TableLoader(
            postgres_db.engine,
            backend,
            table,
            table_to_columns(table),
            incremental=incremental,  # type: ignore[arg-type]
        )
    assert "must be a simple column name" in str(excinfo.value)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_cursor_path_complex_expression(
    postgres_db: PostgresSourceDB, backend: TableBackend
) -> None:
    """Test that a complex JSONPath expression in cursor_path raises a ValueError."""
    table = postgres_db.get_table("chat_message")

    incremental = MockIncremental(
        cursor_path="$.users[0].id",
        last_value=None,
        last_value_func=max,
    )

    with pytest.raises(ValueError) as excinfo:
        TableLoader(
            postgres_db.engine,
            backend,
            table,
            table_to_columns(table),
            incremental=incremental,  # type: ignore[arg-type]
        )
    assert "must be a simple column name" in str(excinfo.value)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("last_value_func", [min, max])
def test_make_query_incremental_range_start_open(
    postgres_db: PostgresSourceDB, backend: TableBackend, last_value_func: Callable[[Any], Any]
) -> None:
    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=last_value_func,
        cursor_path="created_at",
        end_value=None,
        on_cursor_value_missing="raise",
        range_start="open",
    )

    table = postgres_db.get_table("chat_message")

    loader = TableLoader(
        postgres_db.engine,
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
    postgres_db: PostgresSourceDB, backend: TableBackend, last_value_func: Callable[[Any], Any]
) -> None:
    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=last_value_func,
        cursor_path="created_at",
        end_value=None,
        on_cursor_value_missing="raise",
        range_end="closed",
    )

    table = postgres_db.get_table("chat_message")
    loader = TableLoader(
        postgres_db.engine,
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


def test_base_table_loader_is_abstract(postgres_db: PostgresSourceDB) -> None:
    """BaseTableLoader cannot be instantiated because load_rows is abstract."""
    table = postgres_db.get_table("chat_message")
    with pytest.raises(TypeError):
        BaseTableLoader(  # type: ignore[abstract]
            postgres_db.engine,
            "sqlalchemy",
            table,
            table_to_columns(table),
        )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_custom_base_loader_inherits_query_building(
    postgres_db: PostgresSourceDB, backend: TableBackend
) -> None:
    """A minimal BaseTableLoader subclass inherits incremental query building."""

    class CustomLoader(BaseTableLoader):
        def load_rows(self, backend_kwargs=None):
            yield from []

    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=max,
        cursor_path="created_at",
        row_order="asc",
    )

    table = postgres_db.get_table("chat_message")
    loader = CustomLoader(
        postgres_db.engine,
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
def test_connectorx_loader_inherits_query_building(
    postgres_db: PostgresSourceDB, backend: TableBackend
) -> None:
    """ConnectorXTableLoader shares the same incremental query logic as TableLoader."""
    incremental = MockIncremental(
        last_value=dlt.common.pendulum.now(),
        last_value_func=max,
        cursor_path="created_at",
        row_order="asc",
    )

    table = postgres_db.get_table("chat_message")
    # build query with both loader types and compare
    default_loader = TableLoader(
        postgres_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=incremental,  # type: ignore[arg-type]
    )
    cx_loader = ConnectorXTableLoader(
        postgres_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=incremental,  # type: ignore[arg-type]
    )

    assert default_loader.make_query().compare(cx_loader.make_query())


def test_custom_table_loader_overrides_load_rows(
    postgres_db: PostgresSourceDB,
) -> None:
    """A TableLoader subclass can override _load_rows and reuse _convert_result.

    This validates the pagination use case from PR #2477: users override
    _load_rows to change execution strategy while reusing _convert_result
    for backend format conversion.
    """
    # reflect a fresh table to avoid columns appended by other tests
    fresh_metadata = sa.MetaData(schema=postgres_db.schema)
    table = sa.Table("chat_message", fresh_metadata, autoload_with=postgres_db.engine)
    columns = table_to_columns(table)

    class PaginatedLoader(TableLoader):
        """Simulated pagination: executes the query twice with LIMIT/OFFSET."""

        def __init__(self, *args, page_size=1000, **kwargs):
            super().__init__(*args, **kwargs)
            self.page_size = page_size
            self.pages_fetched = 0

        def _load_rows(self, query, backend_kwargs):
            # simple offset-based pagination
            with self.engine.connect() as conn:
                offset = 0
                while True:
                    page_query = query.limit(self.page_size).offset(offset)
                    result = conn.execute(page_query)
                    rows = list(result)
                    if not rows:
                        break
                    self.pages_fetched += 1
                    # reuse _convert_result by wrapping rows back into a result-like obj
                    yield [dict(row._mapping) for row in rows]
                    offset += self.page_size

    loader = PaginatedLoader(
        postgres_db.engine,
        "sqlalchemy",
        table,
        columns,
    )

    all_pages = list(loader.load_rows())
    expected_total = postgres_db.table_infos["chat_message"]["row_count"]
    total_rows = sum(len(page) for page in all_pages)
    assert total_rows == expected_total
    # each page is a list of dicts capped at page_size
    for page in all_pages:
        assert isinstance(page, list)
        assert len(page) <= loader.page_size
    # verify exact page count
    expected_pages = (expected_total + loader.page_size - 1) // loader.page_size
    assert loader.pages_fetched == expected_pages


def test_table_loader_class_in_table_rows(
    postgres_db: PostgresSourceDB,
) -> None:
    """Custom loader class passed to sql_table() is actually used."""

    class TrackingLoader(TableLoader):
        instances: List["TrackingLoader"] = []

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            TrackingLoader.instances.append(self)

    TrackingLoader.instances.clear()

    # run a pipeline to exercise table_rows with the custom class
    pipe = dlt.pipeline(
        pipeline_name="test_loader_class_pipe",
        destination="duckdb",
        dev_mode=True,
    )
    # call sql_table with the custom loader
    from dlt.sources.sql_database import sql_table

    resource = sql_table(
        credentials=postgres_db.engine,
        table="chat_message",
        schema=postgres_db.schema,
        backend="sqlalchemy",
        table_loader_class=TrackingLoader,
    )
    info = pipe.run(resource)
    assert len(info.load_packages) > 0
    assert not info.has_failed_jobs
    # verify our custom loader was instantiated
    assert len(TrackingLoader.instances) > 0
    assert isinstance(TrackingLoader.instances[0], TrackingLoader)


def test_register_table_loader_backend(postgres_db: PostgresSourceDB) -> None:
    """Register a custom backend and verify it appears in registry and Literal."""
    from typing import get_args

    class MyLoader(BaseTableLoader):
        def load_rows(self, backend_kwargs=None):
            yield from []

    saved_args = TableBackend.__args__  # type: ignore[attr-defined]
    try:
        register_table_loader_backend("my_custom", MyLoader)
        assert TABLE_LOADER_REGISTRY["my_custom"] is MyLoader
        assert "my_custom" in get_args(TableBackend)
    finally:
        # clean up
        TABLE_LOADER_REGISTRY.pop("my_custom", None)
        TableBackend.__args__ = saved_args  # type: ignore[attr-defined]


def test_register_table_loader_backend_rejects_non_subclass() -> None:
    """Passing a class that does not subclass BaseTableLoader raises ValueError."""

    class NotALoader:
        pass

    with pytest.raises(ValueError, match="Must be a subclass"):
        register_table_loader_backend("bad", NotALoader)  # type: ignore[arg-type]


def test_get_table_loader_class_builtin() -> None:
    """Built-in backend names resolve to the correct loader classes."""
    assert get_table_loader_class("sqlalchemy") is TableLoader
    assert get_table_loader_class("pyarrow") is TableLoader
    assert get_table_loader_class("pandas") is TableLoader
    assert get_table_loader_class("connectorx") is ConnectorXTableLoader


def test_get_table_loader_class_unknown() -> None:
    """Unknown backend name raises ValueErrorWithKnownValues."""
    from dlt.common.exceptions import ValueErrorWithKnownValues

    with pytest.raises(ValueErrorWithKnownValues):
        get_table_loader_class("nonexistent_backend")


def test_custom_backend_end_to_end(postgres_db: PostgresSourceDB) -> None:
    """Register a custom backend, use it via sql_table(backend=...), verify it loads data."""
    from dlt.sources.sql_database import sql_table

    class TrackingCustomLoader(TableLoader):
        """Custom loader that records it was instantiated."""

        used: bool = False

        def load_rows(self, backend_kwargs=None):
            TrackingCustomLoader.used = True
            return super().load_rows(backend_kwargs)

    saved_args = TableBackend.__args__  # type: ignore[attr-defined]
    TrackingCustomLoader.used = False
    try:
        register_table_loader_backend("tracking_custom", TrackingCustomLoader)

        pipe = dlt.pipeline(
            pipeline_name="test_custom_backend_e2e",
            destination="duckdb",
            dev_mode=True,
        )
        resource = sql_table(
            credentials=postgres_db.engine,
            table="chat_message",
            schema=postgres_db.schema,
            backend="tracking_custom",  # type: ignore[arg-type]
        )
        info = pipe.run(resource)
        assert len(info.load_packages) > 0
        assert not info.has_failed_jobs
        assert TrackingCustomLoader.used
    finally:
        TABLE_LOADER_REGISTRY.pop("tracking_custom", None)
        TableBackend.__args__ = saved_args  # type: ignore[attr-defined]


def test_custom_load_rows_reuses_convert_result(
    postgres_db: PostgresSourceDB,
) -> None:
    """A custom _load_rows override can call _convert_result for format conversion.

    This exercises the pyarrow backend path through _convert_result, proving
    the intended pattern: override execution strategy, reuse conversion.
    """
    # reflect fresh table to avoid columns from other tests
    fresh_metadata = sa.MetaData(schema=postgres_db.schema)
    table = sa.Table("chat_message", fresh_metadata, autoload_with=postgres_db.engine)
    columns = table_to_columns(table)

    class ConvertReuseLoader(TableLoader):
        """Executes query page-by-page and delegates conversion to _convert_result."""

        convert_result_called: int = 0

        def _load_rows(self, query: Any, backend_kwargs: Dict[str, Any]) -> Iterator[TDataItem]:
            with self.engine.connect() as conn:
                result = conn.execution_options(yield_per=self.chunk_size).execute(query)
                try:
                    ConvertReuseLoader.convert_result_called += 1
                    yield from self._convert_result(result, backend_kwargs)
                finally:
                    result.close()

    ConvertReuseLoader.convert_result_called = 0
    loader = ConvertReuseLoader(
        postgres_db.engine,
        "pyarrow",
        table,
        columns,
        chunk_size=5000,
    )

    import pyarrow as pa

    all_batches = list(loader.load_rows())
    expected_total = postgres_db.table_infos["chat_message"]["row_count"]
    total_rows = sum(batch.num_rows for batch in all_batches)
    assert total_rows == expected_total
    # verify every yielded item is an Arrow table
    for batch in all_batches:
        assert isinstance(batch, pa.Table)
    # _convert_result was actually called
    assert ConvertReuseLoader.convert_result_called > 0


def test_table_loader_class_via_sql_database_source(
    postgres_db: PostgresSourceDB,
) -> None:
    """table_loader_class parameter works when passed through sql_database() source."""

    class TrackingLoader(TableLoader):
        table_names_loaded: List[str] = []

        def load_rows(self, backend_kwargs: Dict[str, Any] = None) -> Iterator[TDataItem]:
            TrackingLoader.table_names_loaded.append(self.table.name)
            return super().load_rows(backend_kwargs)

    TrackingLoader.table_names_loaded.clear()

    from dlt.sources.sql_database import sql_database

    pipe = dlt.pipeline(
        pipeline_name="test_loader_class_source",
        destination="duckdb",
        dev_mode=True,
    )
    source = sql_database(
        credentials=postgres_db.engine,
        schema=postgres_db.schema,
        table_names=["chat_message", "app_user"],
        backend="sqlalchemy",
        table_loader_class=TrackingLoader,
    )
    info = pipe.run(source)
    assert len(info.load_packages) > 0
    assert not info.has_failed_jobs
    # verify TrackingLoader was used for both tables
    assert set(TrackingLoader.table_names_loaded) == {"chat_message", "app_user"}


def _get_adbc_postgresql_driver() -> str:
    """Get the ADBC postgresql driver location, or skip if unavailable.

    Uses dlt's own ADBC detection (has_adbc_driver + get_adbc_driver_location)
    so tests can be controlled on CI via the DISABLE_ADBC_DETECTION config setting.
    Temporarily sets DISABLE_ADBC_DETECTION=0 because the test config.toml disables
    it by default — same pattern as test_adbc_loading.py's autouse fixture.
    """
    from dlt.destinations._adbc_jobs import has_adbc_driver
    from dlt.destinations.impl.postgres.factory import get_adbc_driver_location

    # override the default disable from tests/.dlt/config.toml
    saved = os.environ.get("DISABLE_ADBC_DETECTION")
    os.environ["DISABLE_ADBC_DETECTION"] = "0"
    try:
        driver = get_adbc_driver_location()
        found, err = has_adbc_driver(driver)
    finally:
        # restore previous value
        if saved is None:
            os.environ.pop("DISABLE_ADBC_DETECTION", None)
        else:
            os.environ["DISABLE_ADBC_DETECTION"] = saved
    if not found:
        pytest.skip(f"ADBC postgresql driver not available: {err}")
    return driver


def test_adbc_table_loader(postgres_db: PostgresSourceDB) -> None:
    """Example ADBC table loader: reads data as Arrow tables via ADBC.

    This demonstrates subclassing BaseTableLoader for an entirely different
    backend (issue #2736 checklist item 4).  The loader compiles the query
    to a SQL string and executes it through an ADBC connection, yielding
    Arrow tables directly without going through SQLAlchemy row iteration.
    """
    driver = _get_adbc_postgresql_driver()

    import pyarrow as pa

    class AdbcArrowTableLoader(BaseTableLoader):
        """Reads from PostgreSQL via ADBC, yielding Arrow tables.

        Uses compile_query() and get_connection_url() from BaseTableLoader
        so connection string handling can be overridden per database.
        """

        def load_rows(self, backend_kwargs: Dict[str, Any] = None) -> Iterator[TDataItem]:
            from adbc_driver_manager import dbapi

            query = self.make_query()
            query_str = self.compile_query(query)
            conn_url = self.get_connection_url()

            with dbapi.connect(driver=driver, db_kwargs={"uri": conn_url}) as conn:
                with conn.cursor() as cur:
                    cur.execute(query_str)
                    reader = cur.fetch_record_batch()
                    for batch in reader:
                        yield pa.Table.from_batches([batch], schema=batch.schema)

    # reflect fresh table
    fresh_metadata = sa.MetaData(schema=postgres_db.schema)
    table = sa.Table("chat_message", fresh_metadata, autoload_with=postgres_db.engine)
    columns = table_to_columns(table)

    loader = AdbcArrowTableLoader(
        postgres_db.engine,
        "pyarrow",  # backend value doesn't affect this loader's behavior
        table,
        columns,
        chunk_size=5000,
    )

    all_batches = list(loader.load_rows())
    expected_total = postgres_db.table_infos["chat_message"]["row_count"]
    total_rows = sum(batch.num_rows for batch in all_batches)
    assert total_rows == expected_total
    # verify Arrow tables
    for batch in all_batches:
        assert isinstance(batch, pa.Table)
        assert "id" in batch.column_names
        assert "content" in batch.column_names


def test_adbc_table_loader_end_to_end(postgres_db: PostgresSourceDB) -> None:
    """ADBC loader works end-to-end through sql_table + pipeline.

    Registers the ADBC loader as a custom backend and loads data into duckdb.
    """
    driver = _get_adbc_postgresql_driver()

    import pyarrow as pa

    class AdbcArrowTableLoader(BaseTableLoader):
        """Reads from PostgreSQL via ADBC, yielding Arrow tables."""

        def load_rows(self, backend_kwargs: Dict[str, Any] = None) -> Iterator[TDataItem]:
            from adbc_driver_manager import dbapi

            query = self.make_query()
            query_str = self.compile_query(query)
            conn_url = self.get_connection_url()

            with dbapi.connect(driver=driver, db_kwargs={"uri": conn_url}) as conn:
                with conn.cursor() as cur:
                    cur.execute(query_str)
                    reader = cur.fetch_record_batch()
                    for batch in reader:
                        yield pa.Table.from_batches([batch], schema=batch.schema)

    from dlt.sources.sql_database import sql_table

    pipe = dlt.pipeline(
        pipeline_name="test_adbc_e2e",
        destination="duckdb",
        dev_mode=True,
    )
    resource = sql_table(
        credentials=postgres_db.engine,
        table="chat_message",
        schema=postgres_db.schema,
        table_loader_class=AdbcArrowTableLoader,
    )
    info = pipe.run(resource)
    assert len(info.load_packages) > 0
    assert not info.has_failed_jobs

    # verify data arrived
    with pipe.sql_client() as client:
        with client.execute_query("SELECT count(*) FROM chat_message") as cur:
            count = cur.fetchone()[0]
    expected_total = postgres_db.table_infos["chat_message"]["row_count"]
    assert count == expected_total


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
