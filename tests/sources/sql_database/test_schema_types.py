import pytest
from typing import Optional, Union

import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.dialects.oracle import NUMBER
from sqlalchemy.sql.type_api import TypeEngine

from dlt.common.data_types import TDataType
from dlt.sources.sql_database.schema_types import get_table_references, sqla_col_to_column_schema


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


@pytest.mark.parametrize(
    "oracle_type,expected_type,expected_precision,expected_scale,test_value",
    [
        (NUMBER(), "bigint", None, None, 123456789),
        (NUMBER(precision=17), "bigint", 17, None, 9309935020231023),
        (NUMBER(precision=17, scale=0), "bigint", 17, None, 9309935020231023),
        (NUMBER(precision=10, scale=2), "decimal", 10, 2, 12345.67),
        (NUMBER(precision=17, scale=2, asdecimal=False), "double", None, None, 12345.67),
    ],
    ids=["NUMBER", "NUMBER(17)", "NUMBER(17,0)", "NUMBER(10,2)", "NUMBER(17,2,asdecimal='False')"],
)
def test_oracle_number_type_inference(
    oracle_type: TypeEngine,
    expected_type: TDataType,
    expected_precision: Optional[int],
    expected_scale: Optional[int],
    test_value: Union[int, float],
) -> None:
    """Test Oracle NUMBER type inference to prevent PyArrow conversion errors.

    Oracle NUMBER types can represent both integers and decimals based on their scale:
    - NUMBER with scale=0 or no scale → should be inferred as 'bigint'
    - NUMBER with scale>0 → should be inferred as 'decimal'

    Previously, all Oracle NUMBER types were incorrectly inferred as 'double',
    causing PyArrow conversion failures for large integers like 9309935020231023
    that cannot be precisely represented as float64.
    """
    sql_col = sa.Column("test_col", oracle_type, nullable=True)
    column_schema = sqla_col_to_column_schema(sql_col, reflection_level="full_with_precision")

    assert column_schema["data_type"] == expected_type
    if expected_precision is not None:
        assert column_schema.get("precision") == expected_precision
    if expected_scale is not None and expected_type == "decimal":
        assert column_schema.get("scale") == expected_scale

    # the inferred bigint type should work with PyArrow
    if expected_type == "bigint" and test_value == 9309935020231023:
        pa_array = pa.array([test_value], type="int64")
        assert pa_array[0].as_py() == test_value
        with pytest.raises(
            pa.ArrowInvalid, match="Integer value 9309935020231023 is outside of the range"
        ):
            pa.array([test_value], type="float64")
