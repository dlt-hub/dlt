"""Tests for extensible dialect capabilities in the SqlAlchemy destination.

Tests registration, backward compatibility of factory-level adjust_capabilities
for all dialects that had hardcoded behavior, type mapper visitor pattern,
table adapter, error handler, and backward compat with type_mapper= param.
"""

from typing import Optional, Type
from unittest.mock import patch

import pytest

import dlt
import sqlalchemy as sa
from sqlalchemy.sql import sqltypes

from dlt.common.destination.capabilities import DataTypeMapper, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema
from dlt.destinations.impl.sqlalchemy.dialect import (
    DialectCapabilities,
    MysqlDialectCapabilities,
    MssqlDialectCapabilities,
    TrinoDialectCapabilities,
    OracleDialectCapabilities,
    register_dialect_capabilities,
    get_dialect_capabilities,
    DIALECT_CAPS_REGISTRY,
)
from dlt.destinations.impl.sqlalchemy.type_mapper import (
    SqlalchemyTypeMapper,
    MysqlVariantTypeMapper,
    MssqlVariantTypeMapper,
    TrinoVariantTypeMapper,
)
from dlt.destinations.impl.sqlalchemy.db_api_client import SqlalchemyClient
from dlt.destinations.exceptions import DatabaseUndefinedRelation, DatabaseTerminalException

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


# -- registry tests --


def test_register_and_lookup() -> None:
    """Custom capabilities can be registered and looked up."""

    class MyDialectCaps(DialectCapabilities):
        pass

    register_dialect_capabilities("__test_custom_db__", MyDialectCaps)
    try:
        assert isinstance(get_dialect_capabilities("__test_custom_db__"), MyDialectCaps)
    finally:
        DIALECT_CAPS_REGISTRY.pop("__test_custom_db__", None)


def test_lookup_missing_returns_none() -> None:
    assert get_dialect_capabilities("__nonexistent_dialect__") is None


def test_register_invalid_class_raises() -> None:
    with pytest.raises(ValueError, match="must be a subclass of DialectCapabilities"):
        register_dialect_capabilities("bad", object)  # type: ignore[arg-type]


def test_builtin_dialects_registered() -> None:
    """Dialects with custom behavior are pre-registered at import time."""
    for name in ("mysql", "mariadb", "trino", "mssql", "oracle"):
        assert get_dialect_capabilities(name) is not None, f"{name} not registered"


# -- backward compat: factory-level adjust_capabilities --
# These tests verify that the registry-based system produces identical
# capabilities to the old hardcoded if/elif chains for every dialect
# that had custom behavior. Each test creates a destination via the factory
# and inspects the resulting capabilities.


def test_factory_caps_mysql() -> None:
    """MySQL capabilities match the old hardcoded behavior."""
    dest = dlt.destinations.sqlalchemy(credentials="mysql+pymysql://u:p@localhost/db")
    caps = dest.capabilities()

    assert caps.sqlglot_dialect == "mysql"
    assert caps.max_identifier_length == 64
    assert caps.max_column_identifier_length == 64
    assert caps.enforces_nulls_on_alter is False
    assert isinstance(caps.dialect_capabilities, MysqlDialectCapabilities)
    assert issubclass(caps.type_mapper, MysqlVariantTypeMapper)
    # format_datetime_literal must be the mysql variant (no_tz=True)
    from dlt.destinations.impl.sqlalchemy.dialect import _format_mysql_datetime_literal

    assert caps.format_datetime_literal is _format_mysql_datetime_literal


def test_factory_caps_mariadb() -> None:
    """MariaDB shares MySQL capabilities."""
    # mariadb reuses pymysql driver
    dest = dlt.destinations.sqlalchemy(credentials="mariadb+pymysql://u:p@localhost/db")
    caps = dest.capabilities()

    assert caps.sqlglot_dialect == "mysql"
    assert caps.max_identifier_length == 64
    assert caps.max_column_identifier_length == 64
    assert isinstance(caps.dialect_capabilities, MysqlDialectCapabilities)
    assert issubclass(caps.type_mapper, MysqlVariantTypeMapper)


def test_factory_caps_mssql() -> None:
    """MSSQL capabilities match the old hardcoded behavior."""
    dest = dlt.destinations.sqlalchemy(credentials="mssql+pyodbc://u:p@localhost/db")
    caps = dest.capabilities()

    assert caps.sqlglot_dialect == "tsql"
    assert caps.max_identifier_length == 128
    assert caps.max_column_identifier_length == 128
    assert isinstance(caps.dialect_capabilities, MssqlDialectCapabilities)
    assert issubclass(caps.type_mapper, MssqlVariantTypeMapper)


def test_factory_caps_postgresql() -> None:
    """PostgreSQL capabilities: sqlglot mapped to 'postgres', default type mapper."""
    dest = dlt.destinations.sqlalchemy(credentials="postgresql://u:p@localhost/db")
    caps = dest.capabilities()

    # postgresql -> postgres in SQLGLOT_DIALECTS
    assert caps.sqlglot_dialect == "postgres"
    assert caps.max_identifier_length == 63
    assert caps.max_column_identifier_length == 63
    assert caps.has_case_sensitive_identifiers is True
    # no registered dialect caps -> falls back to base DialectCapabilities
    assert type(caps.dialect_capabilities) is DialectCapabilities
    assert issubclass(caps.type_mapper, SqlalchemyTypeMapper)


def test_factory_caps_sqlite() -> None:
    """SQLite capabilities: identity sqlglot mapping, default type mapper."""
    dest = dlt.destinations.sqlalchemy(credentials="sqlite:///:memory:")
    caps = dest.capabilities()

    assert caps.sqlglot_dialect == "sqlite"
    # sqlite dialect reports max_identifier_length = 9999
    assert caps.max_identifier_length == 9999
    assert caps.supports_native_boolean is False
    assert type(caps.dialect_capabilities) is DialectCapabilities
    assert issubclass(caps.type_mapper, SqlalchemyTypeMapper)


def test_factory_caps_always_sets_dialect_capabilities() -> None:
    """capabilities.dialect_capabilities is always set, never None."""
    for url in [
        "sqlite:///:memory:",
        "mysql+pymysql://u:p@localhost/db",
        "postgresql://u:p@localhost/db",
        "mssql+pyodbc://u:p@localhost/db",
    ]:
        dest = dlt.destinations.sqlalchemy(credentials=url)
        caps = dest.capabilities()
        assert caps.dialect_capabilities is not None, f"dialect_capabilities is None for {url}"
        assert isinstance(caps.dialect_capabilities, DialectCapabilities)


# -- trino: tested via DialectCapabilities class directly (driver not available) --


def test_trino_dialect_caps() -> None:
    """TrinoDialectCapabilities matches the old hardcoded Trino behavior."""
    dc = TrinoDialectCapabilities("trino")

    # sqlglot_dialect is identity for trino
    assert dc.sqlglot_dialect == "trino"

    # adjust_capabilities sets timestamp precision
    caps = DestinationCapabilitiesContext.generic_capabilities()
    dc.adjust_capabilities(caps, sa.engine.default.DefaultDialect())
    assert caps.timestamp_precision == 3
    assert caps.max_timestamp_precision == 3

    # type mapper
    assert issubclass(dc.type_mapper_class(), TrinoVariantTypeMapper)


# -- oracle: factory test on SA 2.0+, direct class test on SA 1.x --


def test_factory_caps_oracle() -> None:
    """Oracle capabilities via factory. Requires SA 2.0+ (oracledb driver plugin)."""
    from dlt.common.utils import assert_min_pkg_version
    from dlt.common.exceptions import DependencyVersionException

    try:
        assert_min_pkg_version(pkg_name="sqlalchemy", version="2.0.0")
    except DependencyVersionException:
        pytest.skip("oracle+oracledb driver requires sqlalchemy >= 2.0")

    dest = dlt.destinations.sqlalchemy(credentials="oracle+oracledb://u:p@localhost/db")
    caps = dest.capabilities()

    assert caps.sqlglot_dialect == "oracle"
    assert caps.max_identifier_length == 128
    assert caps.max_column_identifier_length == 128
    assert isinstance(caps.dialect_capabilities, OracleDialectCapabilities)
    assert issubclass(caps.type_mapper, SqlalchemyTypeMapper)


def test_oracle_dialect_caps() -> None:
    """OracleDialectCapabilities matches the old hardcoded Oracle behavior."""
    dc = OracleDialectCapabilities("oracle")

    assert dc.sqlglot_dialect == "oracle"

    # ORA-00942 detection
    assert dc.is_undefined_relation(Exception("ORA-00942: table or view does not exist")) is True
    # generic patterns also match through super()
    assert dc.is_undefined_relation(Exception("relation does not exist")) is True
    # unrelated error returns None
    assert dc.is_undefined_relation(Exception("syntax error")) is None


# -- sqlglot_dialect mapping --


def test_sqlglot_dialect_explicit_mappings() -> None:
    """SQLGLOT_DIALECTS maps backend names that differ from their sqlglot dialect."""
    for backend, expected in [
        ("postgresql", "postgres"),
        ("mssql", "tsql"),
        ("mariadb", "mysql"),
        ("awsathena", "athena"),
        ("teradatasql", "teradata"),
    ]:
        dc = DialectCapabilities(backend)
        assert dc.sqlglot_dialect == expected, f"{backend} -> {expected}"


def test_sqlglot_dialect_identity_fallback() -> None:
    """When backend name is not in SQLGLOT_DIALECTS, property returns backend name."""
    for backend in (
        "mysql",
        "sqlite",
        "trino",
        "oracle",
        "hive",
        "redshift",
        "clickhouse",
        "databricks",
        "bigquery",
        "snowflake",
        "doris",
        "risingwave",
        "starrocks",
        "drill",
        "druid",
        "presto",
    ):
        dc = DialectCapabilities(backend)
        assert dc.sqlglot_dialect == backend, f"{backend} identity mismatch"


# -- type mapper visitor pattern --


def test_type_mapper_visitor_dispatch() -> None:
    """to_destination_type dispatches to per-type visitor methods."""
    caps = DestinationCapabilitiesContext.generic_capabilities()
    mapper = SqlalchemyTypeMapper(caps)

    col_text: TColumnSchema = {"name": "c", "data_type": "text"}
    assert isinstance(mapper.to_destination_type(col_text), (sa.Text, sa.String))

    col_bool: TColumnSchema = {"name": "c", "data_type": "bool"}
    assert isinstance(mapper.to_destination_type(col_bool), sa.Boolean)

    col_bigint: TColumnSchema = {"name": "c", "data_type": "bigint"}
    assert isinstance(mapper.to_destination_type(col_bigint), sa.BigInteger)

    col_date: TColumnSchema = {"name": "c", "data_type": "date"}
    assert isinstance(mapper.to_destination_type(col_date), sa.Date)


def test_type_mapper_visitor_single_override() -> None:
    """Subclass can override a single visitor method without reimplementing to_destination_type."""

    class MyMapper(SqlalchemyTypeMapper):
        def _db_type_from_json_type(
            self, column: TColumnSchema, table: PreparedTableSchema
        ) -> sqltypes.TypeEngine:
            return sa.String(length=999)

    caps = DestinationCapabilitiesContext.generic_capabilities()
    mapper = MyMapper(caps)

    col_json: TColumnSchema = {"name": "c", "data_type": "json"}
    result = mapper.to_destination_type(col_json)
    assert isinstance(result, sa.String)
    assert result.length == 999

    # other types still work via the base class
    col_bool: TColumnSchema = {"name": "c", "data_type": "bool"}
    assert isinstance(mapper.to_destination_type(col_bool), sa.Boolean)


def test_type_mapper_full_override_backward_compat() -> None:
    """Existing subclass that overrides to_destination_type() directly still works."""

    class LegacyMapper(SqlalchemyTypeMapper):
        def to_destination_type(self, column, table=None):
            if column["data_type"] == "json":
                return sa.String(length=777)
            return super().to_destination_type(column, table)

    caps = DestinationCapabilitiesContext.generic_capabilities()
    mapper = LegacyMapper(caps)

    col_json: TColumnSchema = {"name": "c", "data_type": "json"}
    result = mapper.to_destination_type(col_json)
    assert isinstance(result, sa.String)
    assert result.length == 777

    col_text: TColumnSchema = {"name": "c", "data_type": "text"}
    assert isinstance(mapper.to_destination_type(col_text), (sa.Text, sa.String))


# -- custom adjust_capabilities --


def test_custom_adjust_capabilities() -> None:
    """Custom dialect capabilities adjusts caps correctly."""

    class MyDialectCaps(DialectCapabilities):
        def adjust_capabilities(
            self,
            caps: DestinationCapabilitiesContext,
            dialect: sa.engine.interfaces.Dialect,
        ) -> None:
            caps.max_identifier_length = 42
            caps.max_column_identifier_length = 42

    caps = DestinationCapabilitiesContext.generic_capabilities()
    my_caps = MyDialectCaps()
    my_caps.adjust_capabilities(caps, sa.engine.default.DefaultDialect())
    assert caps.max_identifier_length == 42
    assert caps.max_column_identifier_length == 42


# -- is_undefined_relation hook --


def _make_client_with_dialect_caps(
    dialect_caps: DialectCapabilities,
) -> "tuple[SqlalchemyClient, sa.engine.Engine]":
    """Create a minimal SqlalchemyClient backed by an in-memory sqlite engine.

    Returns (client, engine). Caller MUST dispose the engine when done.
    """
    engine = sa.create_engine("sqlite:///:memory:")
    creds_cls = type(
        "FakeCreds",
        (),
        {
            "engine": engine,
            "database": ":memory:",
            "borrow_conn": lambda self: engine.connect(),
            "return_conn": lambda self, c: c.close(),
        },
    )
    caps = DestinationCapabilitiesContext.generic_capabilities()
    # pass dialect capabilities through the capabilities object
    caps.dialect_capabilities = dialect_caps
    client = SqlalchemyClient("main", "main_staging", creds_cls(), caps)
    return client, engine


def test_is_undefined_relation_custom_true() -> None:
    """Custom handler returning True is recognized."""

    class CustomUndef(DialectCapabilities):
        def is_undefined_relation(self, e: Exception) -> Optional[bool]:
            if "MY_CUSTOM_ERROR" in str(e):
                return True
            return None

    caps_obj = CustomUndef()
    assert caps_obj.is_undefined_relation(Exception("MY_CUSTOM_ERROR xyz")) is True
    # unrelated exception falls through
    assert caps_obj.is_undefined_relation(Exception("something else")) is None


def test_is_undefined_relation_false_in_db_api_client() -> None:
    """Custom handler returning False prevents DatabaseUndefinedRelation in the client."""

    class NeverUndefined(DialectCapabilities):
        def is_undefined_relation(self, e: Exception) -> Optional[bool]:
            return False

    # "no such table" would normally match the generic undefined-relation pattern
    orig_exc = sa.exc.ProgrammingError("stmt", {}, Exception("no such table xyz"))

    client, engine = _make_client_with_dialect_caps(NeverUndefined())
    try:
        result = client._make_database_exception(orig_exc)
        assert not isinstance(result, DatabaseUndefinedRelation)
        assert isinstance(result, DatabaseTerminalException)
    finally:
        engine.dispose()


def test_is_undefined_relation_none_falls_through() -> None:
    """When handler returns None, generic patterns in db_api_client still match."""
    orig_exc = sa.exc.ProgrammingError("stmt", {}, Exception("relation xyz does not exist"))

    client, engine = _make_client_with_dialect_caps(DialectCapabilities())
    try:
        result = client._make_database_exception(orig_exc)
        assert isinstance(result, DatabaseUndefinedRelation)
    finally:
        engine.dispose()


def test_base_dialect_caps_generic_patterns() -> None:
    """Base DialectCapabilities matches generic undefined-relation patterns from many dialects."""
    dc = DialectCapabilities()

    for msg in (
        "relation does not exist",  # PostgreSQL / Trino
        "table not found",  # Hive
        "is an undefined name",  # DB2
        "no such table: foo",  # SQLite
        "unknown database bar",  # MySQL
        "table 'baz' doesn't exist",  # MySQL
        "invalid object name 'x'",  # MSSQL
        "invalid schema name",  # SAP HANA
    ):
        assert dc.is_undefined_relation(Exception(msg)) is True, f"should match: {msg}"

    # unrelated error -> None
    assert dc.is_undefined_relation(Exception("syntax error near SELECT")) is None


# -- adapt_table --


def test_adapt_table_callback() -> None:
    """adapt_table can modify the table object before it is returned."""

    class TagCaps(DialectCapabilities):
        def adapt_table(self, table: sa.Table, table_schema: PreparedTableSchema) -> sa.Table:
            table.info["adapted"] = True
            return table

    metadata = sa.MetaData()
    tbl = sa.Table("test", metadata, sa.Column("a", sa.Integer()), sa.Column("b", sa.Text()))
    schema_table: PreparedTableSchema = {
        "name": "test",
        "columns": {
            "a": {"name": "a", "data_type": "bigint"},
            "b": {"name": "b", "data_type": "text"},
        },
    }
    result = TagCaps().adapt_table(tbl, schema_table)
    assert result.info.get("adapted") is True


# integration tests with pipeline


def _get_backend_name(destination_config: DestinationTestConfiguration) -> str:
    """Extract the backend name from a destination test configuration."""
    from sqlalchemy.engine.url import make_url

    creds = destination_config.credentials
    if isinstance(creds, str):
        url = make_url(creds)
        return url.get_backend_name()
    return creds.get_backend_name()  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["sqlalchemy"]),
    ids=lambda x: x.name,
)
def test_custom_dialect_capabilities_with_pipeline(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Full integration: register custom capabilities that override a single type,
    run a pipeline, and verify data is loaded correctly."""
    from dlt.common import json

    # a custom type mapper that stores json as string
    class CustomJsonString(sa.TypeDecorator):
        impl = sa.String
        cache_ok = True

        def process_bind_param(self, value, dialect):
            if value is None:
                return None
            return json.dumps(value)

        def process_result_value(self, value, dialect):
            if value is None:
                return None
            return json.loads(value)

    class TestMapper(SqlalchemyTypeMapper):
        def _db_type_from_json_type(self, column, table=None):
            return CustomJsonString(length=512)

    backend = _get_backend_name(destination_config)

    # save original registration so we can restore it
    original = DIALECT_CAPS_REGISTRY.get(backend)

    class TestCaps(DialectCapabilities):
        def adjust_capabilities(
            self, caps: DestinationCapabilitiesContext, dialect: sa.engine.interfaces.Dialect
        ) -> None:
            # delegate to original dialect caps so sqlglot_dialect etc. are preserved
            if original is not None:
                original().adjust_capabilities(caps, dialect)

        def type_mapper_class(self) -> Optional[Type[DataTypeMapper]]:
            return TestMapper

    register_dialect_capabilities(backend, TestCaps)
    try:
        dest_ = dlt.destinations.sqlalchemy(credentials=destination_config.credentials)  # type: ignore[arg-type]
        pipeline = destination_config.setup_pipeline(
            "test_custom_dialect_caps", destination=dest_, dev_mode=True
        )

        @dlt.resource(columns={"json_col": {"data_type": "json"}})
        def test_data():
            yield {"id": 1, "json_col": {"key": "value"}, "text_col": "hello"}

        pipeline.run(test_data(), table_name="dialect_caps_test")

        dataset = pipeline.dataset()
        result = dataset.dialect_caps_test.select("id", "json_col", "text_col").fetchall()
        assert len(result) == 1
        assert result[0][0] == 1
        assert result[0][2] == "hello"

        # json should be stored as string
        json_val = result[0][1]
        if isinstance(json_val, str):
            parsed = json.loads(json_val)
            assert parsed["key"] == "value"
        else:
            raise AssertionError("json_col should be stored as string with custom mapper")

    finally:
        # restore original registration
        if original is not None:
            DIALECT_CAPS_REGISTRY[backend] = original
        else:
            DIALECT_CAPS_REGISTRY.pop(backend, None)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["sqlalchemy"]),
    ids=lambda x: x.name,
)
def test_type_mapper_param_overrides_dialect_caps(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Backward compat: passing type_mapper= directly still overrides dialect capabilities."""
    from dlt.common import json
    from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyCredentials

    class CustomJsonStr(sa.TypeDecorator):
        impl = sa.String
        cache_ok = True

        def process_bind_param(self, value, dialect):
            return json.dumps(value) if value is not None else None

        def process_result_value(self, value, dialect):
            return json.loads(value) if value is not None else None

    class DirectMapper(SqlalchemyTypeMapper):
        def _db_type_from_json_type(self, column, table=None):
            return CustomJsonStr(length=256)

    # pass type_mapper directly -- this should override whatever dialect capabilities set
    dest_ = dlt.destinations.sqlalchemy(
        credentials=destination_config.credentials, type_mapper=DirectMapper  # type: ignore[arg-type]
    )
    pipeline = destination_config.setup_pipeline(
        "test_type_mapper_override", destination=dest_, dev_mode=True
    )

    @dlt.resource(columns={"json_col": {"data_type": "json"}})
    def test_data():
        yield {"id": 1, "json_col": {"a": 1}}

    pipeline.run(test_data(), table_name="override_test")

    dataset = pipeline.dataset()
    result = dataset.override_test.select("id", "json_col").fetchall()
    assert len(result) == 1
    json_val = result[0][1]
    if isinstance(json_val, str):
        assert json.loads(json_val)["a"] == 1
    else:
        raise AssertionError("json_col should be stored as string with direct type_mapper")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["sqlalchemy"]),
    ids=lambda x: x.name,
)
def test_adapt_table_reorder_columns(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Integration test: adapt_table reorders columns so PK columns come first.

    This mirrors the real StarRocks use case (#2449) where primary key columns
    must precede all other columns in the physical table definition.
    """

    class ReorderPKFirstCaps(DialectCapabilities):
        """Move primary-key columns to the front of the column list."""

        def adapt_table(self, table: sa.Table, table_schema: PreparedTableSchema) -> sa.Table:
            pk_col_names = [c.name for c in table.primary_key.columns]
            if not pk_col_names:
                return table
            # split into pk and non-pk columns, preserving relative order within each group
            pk_cols = [c for c in table.columns if c.name in pk_col_names]
            other_cols = [c for c in table.columns if c.name not in pk_col_names]
            reordered = pk_cols + other_cols
            if [c.name for c in table.columns] == [c.name for c in reordered]:
                return table  # already in order

            # rebuild the table with reordered columns
            schema = table.schema
            name = table.name
            metadata = table.metadata  # type: ignore[attr-defined]
            metadata.remove(table)
            return sa.Table(
                name,
                metadata,
                *[c.copy() for c in reordered],
                sa.PrimaryKeyConstraint(*pk_col_names),
                schema=schema,
            )

    backend = _get_backend_name(destination_config)

    original = DIALECT_CAPS_REGISTRY.get(backend)
    register_dialect_capabilities(backend, ReorderPKFirstCaps)
    try:
        dest_ = dlt.destinations.sqlalchemy(
            credentials=destination_config.credentials,  # type: ignore[arg-type]
            create_primary_keys=True,
        )
        pipeline = destination_config.setup_pipeline(
            "test_adapt_reorder", destination=dest_, dev_mode=True
        )

        # define columns with id intentionally last -- the adapter should move it first
        @dlt.resource(
            columns={
                "text_col": {"data_type": "text", "nullable": False},
                "val": {"data_type": "bigint", "nullable": False},
                "id": {"data_type": "bigint", "primary_key": True, "nullable": False},
            },
            primary_key="id",
        )
        def pk_test():
            yield {"text_col": "hello", "val": 10, "id": 1}
            yield {"text_col": "world", "val": 20, "id": 2}

        pipeline.run(pk_test(), table_name="reorder_test")

        # reflect the physical table and verify column order: id must be first
        with pipeline.sql_client() as client:
            reflected = client.reflect_table("reorder_test")
            col_names = [c.name for c in reflected.columns]
            id_pos = col_names.index("id")
            text_pos = col_names.index("text_col")
            val_pos = col_names.index("val")
            assert (
                id_pos < text_pos
            ), f"PK column 'id' at position {id_pos} should precede 'text_col' at {text_pos}"
            assert (
                id_pos < val_pos
            ), f"PK column 'id' at position {id_pos} should precede 'val' at {val_pos}"

        # verify data loads correctly and is readable
        dataset = pipeline.dataset()
        rows = dataset.reorder_test.select("id", "text_col", "val").fetchall()
        assert len(rows) == 2
        rows_by_id = {r[0]: r for r in rows}
        assert rows_by_id[1] == (1, "hello", 10)
        assert rows_by_id[2] == (2, "world", 20)
    finally:
        if original is not None:
            DIALECT_CAPS_REGISTRY[backend] = original
        else:
            DIALECT_CAPS_REGISTRY.pop(backend, None)
