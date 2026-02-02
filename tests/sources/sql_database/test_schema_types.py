import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.dialects.postgresql import UUID

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
