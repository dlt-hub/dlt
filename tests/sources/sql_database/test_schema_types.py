from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.mssql import DATETIMEOFFSET, UNIQUEIDENTIFIER
from sqlalchemy.dialects.oracle import DATE as OracleDATE
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.dialects.postgresql import UUID

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import TimezoneContext

from dlt.sources.sql_database.schema_types import (
    default_table_adapter,
    get_table_references,
    sqla_col_to_column_schema,
    _is_uuid_type,
)


@pytest.mark.parametrize(
    "sql_type,expected",
    [
        (UNIQUEIDENTIFIER(), True),
        (UUID(), True),
        (sa.String(), False),
        (sa.Integer(), False),
        (sa.DateTime(), False),
    ],
    ids=["uniqueidentifier", "pg_uuid", "string", "integer", "datetime"],
)
def test_is_uuid_type(sql_type: sa.types.TypeEngine, expected: bool) -> None:
    """_is_uuid_type detects UUID-like types across SA versions."""
    assert _is_uuid_type(sql_type) is expected


def test_is_uuid_type_sa2_generic_uuid() -> None:
    """_is_uuid_type detects the generic sa.Uuid introduced in SA 2.0."""
    sa_uuid = pytest.importorskip("sqlalchemy", minversion="2.0")
    sql_t = sa_uuid.Uuid()
    assert _is_uuid_type(sql_t) is True


@pytest.mark.parametrize(
    "sql_type",
    [UNIQUEIDENTIFIER(), UUID()],
    ids=["uniqueidentifier", "pg_uuid"],
)
def test_uuid_mapped_to_text(sql_type: sa.types.TypeEngine) -> None:
    """sqla_col_to_column_schema maps UUID-like types to data_type='text'."""
    metadata = sa.MetaData()
    table = sa.Table("t", metadata, sa.Column("col", sql_type))
    col_schema = sqla_col_to_column_schema(table.c.col, "full")
    assert col_schema is not None
    assert col_schema["data_type"] == "text"


def test_uuid_mapped_to_text_sa2_generic_uuid() -> None:
    """sqla_col_to_column_schema maps generic sa.Uuid (SA 2.0) to data_type='text'."""
    sa_uuid = pytest.importorskip("sqlalchemy", minversion="2.0")
    metadata = sa.MetaData()
    table = sa.Table("t", metadata, sa.Column("col", sa_uuid.Uuid()))
    col_schema = sqla_col_to_column_schema(table.c.col, "full")
    assert col_schema is not None
    assert col_schema["data_type"] == "text"


@pytest.mark.parametrize(
    "sql_type",
    [UUID(as_uuid=True), UNIQUEIDENTIFIER()],
    ids=["pg_uuid", "uniqueidentifier"],
)
def test_default_table_adapter_uuid(sql_type: sa.types.TypeEngine) -> None:
    """default_table_adapter sets as_uuid=False when the attribute exists, never crashes.

    PG UUID always has as_uuid. MSSQL UNIQUEIDENTIFIER has it on SA 2.0 only.
    """
    metadata = sa.MetaData()
    table = sa.Table("t", metadata, sa.Column("col", sql_type))
    # must not raise on any SA version
    default_table_adapter(table, included_columns=None)
    if hasattr(table.c.col.type, "as_uuid"):
        assert table.c.col.type.as_uuid is False


def test_default_table_adapter_sa2_generic_uuid() -> None:
    """default_table_adapter sets as_uuid=False on generic sa.Uuid (SA 2.0)."""
    sa_uuid = pytest.importorskip("sqlalchemy", minversion="2.0")
    metadata = sa.MetaData()
    table = sa.Table("t", metadata, sa.Column("col", sa_uuid.Uuid()))
    default_table_adapter(table, included_columns=None)
    assert table.c.col.type.as_uuid is False


@pytest.mark.parametrize(
    "sql_type,expected_tz",
    [
        (sa.DateTime(timezone=False), False),
        (sa.DateTime(timezone=True), True),
        (OracleDATE(), False),
        (DATETIMEOFFSET(), True),
    ],
    ids=["datetime_no_tz", "datetime_tz", "oracle_date", "mssql_datetimeoffset"],
)
def test_datetime_timezone_mapping(sql_type: sa.types.TypeEngine, expected_tz: bool) -> None:
    """sqla_col_to_column_schema maps timezone correctly for DateTime types."""
    metadata = sa.MetaData()
    table = sa.Table("t", metadata, sa.Column("col", sql_type))
    col_schema = sqla_col_to_column_schema(table.c.col, "full")
    assert col_schema is not None
    assert col_schema["timezone"] is expected_tz


@pytest.mark.parametrize(
    "kwargs,expected_tz",
    [({"timezone": True}, True), ({"local_timezone": True}, True), ({}, False)],
    ids=["tstz", "ltz", "plain"],
)
def test_oracle_timestamp_timezone_mapping(kwargs: Dict[str, Any], expected_tz: bool) -> None:
    # `local_timezone` is SQLAlchemy 2.0+
    pytest.importorskip("sqlalchemy", minversion="2.0")
    from sqlalchemy.dialects.oracle import TIMESTAMP as OracleTIMESTAMP

    table = sa.Table("t", sa.MetaData(), sa.Column("col", OracleTIMESTAMP(**kwargs)))
    col_schema = sqla_col_to_column_schema(table.c.col, "full")
    assert col_schema["data_type"] == "timestamp"
    assert col_schema["timezone"] is expected_tz


@pytest.mark.parametrize("local_timezone", [False, True], ids=["tstz", "ltz"])
def test_oracle_utc_timestamp_query(local_timezone: bool) -> None:
    """Only the select list is converted, the filter and the order use the raw (indexed) column."""
    if local_timezone:
        pytest.importorskip("sqlalchemy", minversion="2.0")
    from dlt.sources.sql_database.helpers import OracleUTCTimestamp

    table = sa.Table("t", sa.MetaData(), sa.Column("col", OracleUTCTimestamp(local_timezone)))
    cutoff = datetime(2024, 7, 2, 13, 45, 10, 123456, tzinfo=timezone(timedelta(hours=2)))
    query = sa.select(table.c.col).where(table.c.col > cutoff).order_by(table.c.col)

    sql = " ".join(
        str(query.compile(dialect=OracleDialect(), compile_kwargs={"literal_binds": True})).split()
    )
    bind = (
        "from_tz(CAST(TO_TIMESTAMP('2024-07-02 11:45:10.123456', 'YYYY-MM-DD HH24:MI:SS.FF6')"
        " AS TIMESTAMP), '+00:00')"
    )
    if local_timezone:
        bind = f"CAST({bind} AS TIMESTAMP WITH LOCAL TIME ZONE)"
    assert sql == f"SELECT sys_extract_utc(t.col) AS col FROM t WHERE t.col > {bind} ORDER BY t.col"


def test_oracle_utc_timestamp_values() -> None:
    from dlt.sources.sql_database.helpers import OracleUTCTimestamp

    utc_type = OracleUTCTimestamp()
    # bound values are sent as naive UTC, a naive value is in the context timezone
    aware = datetime(2024, 7, 2, 13, 45, 10, 123456, tzinfo=timezone(timedelta(hours=2)))
    assert utc_type.process_bind_param(aware, None) == datetime(2024, 7, 2, 11, 45, 10, 123456)
    naive = datetime(2024, 7, 2, 11, 45, 10)
    assert utc_type.process_bind_param(naive, None) == naive
    with Container().injectable_context(TimezoneContext("Europe/Warsaw")):
        assert utc_type.process_bind_param(naive, None) == datetime(2024, 7, 2, 9, 45, 10)
    # SYS_EXTRACT_UTC returns naive UTC, handed out as aware
    assert utc_type.process_result_value(naive, None) == naive.replace(tzinfo=timezone.utc)


def test_type_decorator_is_unwrapped() -> None:
    class UTCTimestamp(sa.TypeDecorator):
        impl = sa.TIMESTAMP
        cache_ok = True

        def __init__(self) -> None:
            super().__init__(timezone=True)

    metadata = sa.MetaData()
    table = sa.Table("t", metadata, sa.Column("col", UTCTimestamp()))
    col_schema = sqla_col_to_column_schema(table.c.col, "full")
    assert col_schema is not None
    assert col_schema["data_type"] == "timestamp"
    assert col_schema["timezone"] is True


@pytest.mark.parametrize(
    "sql_type_name,kwargs,swapped",
    [
        ("oracle_timestamp", {"timezone": True}, True),
        ("oracle_timestamp", {"local_timezone": True}, True),
        ("oracle_timestamp", {}, False),
        ("oracle_date", {}, False),
    ],
    ids=["tstz", "ltz", "plain_timestamp", "date"],
)
def test_oracle_reflect_listener_swaps_only_zoned_timestamps(
    sql_type_name: str, kwargs: Dict[str, Any], swapped: bool
) -> None:
    # a plain TIMESTAMP would be shifted by the session offset and DATE is rejected outright,
    # and Oracle DATE is easy to catch by accident since it is a SQLAlchemy DateTime subclass
    pytest.importorskip("sqlalchemy", minversion="2.0")
    from sqlalchemy.dialects.oracle import TIMESTAMP as OracleTIMESTAMP

    from dlt.sources.sql_database.helpers import (
        OracleUTCTimestamp,
        _oracle_column_reflect_listener,
    )

    sql_type = OracleDATE() if sql_type_name == "oracle_date" else OracleTIMESTAMP(**kwargs)
    column_info: Dict[str, Any] = {"name": "col", "type": sql_type}
    _oracle_column_reflect_listener(None, None, column_info)
    assert isinstance(column_info["type"], OracleUTCTimestamp) is swapped
    if not swapped:
        assert column_info["type"] is sql_type


def test_get_table_references() -> None:
    # Test converting foreign keys to reference hints
    metadata = sa.MetaData()

    parent = sa.Table(
        "parent",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
    )

    child = sa.Table(
        "child",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("parent_id", sa.Integer, sa.ForeignKey("parent.id")),
    )

    refs = get_table_references(parent)
    assert refs == []

    refs = get_table_references(child)
    assert refs == [
        {
            "columns": ["parent_id"],
            "referenced_table": "parent",
            "referenced_columns": ["id"],
        }
    ]

    # When referred table has not been reflected the reference is not resolved
    metadata = sa.MetaData()
    child = child.tometadata(metadata)

    refs = get_table_references(child)

    # Refs are not resolved
    assert refs == []

    # Multiple fks to the same table are merged into one reference
    metadata = sa.MetaData()

    parent = sa.Table(
        "parent",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("country", sa.String),
        sa.UniqueConstraint("id", "country"),
    )
    parent_2 = sa.Table(  # noqa: F841
        "parent_2",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
    )
    child = sa.Table(
        "child",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("country", sa.String),
        sa.Column("parent_id", sa.Integer, sa.ForeignKey("parent.id")),
        sa.Column("parent_country", sa.String, sa.ForeignKey("parent.country")),
        sa.Column("parent_2_id", sa.Integer, sa.ForeignKey("parent_2.id")),
    )
    refs = get_table_references(child)
    refs = sorted(refs, key=lambda x: x["referenced_table"])
    assert refs[0]["referenced_table"] == "parent"
    # Sqla constraints are not in fixed order
    assert set(refs[0]["columns"]) == {"parent_id", "parent_country"}
    assert set(refs[0]["referenced_columns"]) == {"id", "country"}
    # Ensure columns and referenced columns are the same order
    col_mapping = {
        col: ref_col for col, ref_col in zip(refs[0]["columns"], refs[0]["referenced_columns"])
    }
    expected_col_mapping = {"parent_id": "id", "parent_country": "country"}
    assert col_mapping == expected_col_mapping

    assert refs[1] == {
        "columns": ["parent_2_id"],
        "referenced_table": "parent_2",
        "referenced_columns": ["id"],
    }

    # Compsite foreign keys give one reference
    metadata = sa.MetaData()
    parent.to_metadata(metadata)
    child = sa.Table(
        "child",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("parent_id", sa.Integer),
        sa.Column("parent_country", sa.String),
        sa.ForeignKeyConstraint(["parent_id", "parent_country"], ["parent.id", "parent.country"]),
    )

    refs = get_table_references(child)
    assert refs[0]["referenced_table"] == "parent"
    col_mapping = {
        col: ref_col for col, ref_col in zip(refs[0]["columns"], refs[0]["referenced_columns"])
    }
    expected_col_mapping = {"parent_id": "id", "parent_country": "country"}
    assert col_mapping == expected_col_mapping

    # Foreign key to different schema is not resolved
    metadata = sa.MetaData()
    parent = parent.tometadata(metadata, schema="first_schema")
    child = sa.Table(
        "child",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("parent_id", sa.Integer, sa.ForeignKey("first_schema.parent.id")),
    )

    refs = get_table_references(child)
    assert refs == []
