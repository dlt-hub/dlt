import sqlalchemy as sa

from dlt.sources.sql_database.schema_types import get_table_references


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
    # Sqla aonstraints are not in fixed order
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
