import pytest
from dlt.destinations.impl.clickhouse.factory import ClickHouseTypeMapper
from dlt.common.schema.typing import TColumnSchema


@pytest.mark.parametrize(
    "column,expected_type",
    [
        (TColumnSchema(precision=None), "Int64"),
        (TColumnSchema(precision=8), "Int8"),
        (TColumnSchema(precision=16), "Int16"),
        (TColumnSchema(precision=32), "Int32"),
        (TColumnSchema(precision=64), "Int64"),
        (TColumnSchema(), "Int64"),  # No precision key at all
    ],
)
def test_to_db_integer_type_valid(column, expected_type):
    mapper = ClickHouseTypeMapper(capabilities=None)
    assert mapper.to_db_integer_type(column) == expected_type


def test_to_db_integer_type_invalid():
    mapper = ClickHouseTypeMapper(capabilities=None)
    with pytest.raises(ValueError):
        mapper.to_db_integer_type(TColumnSchema(precision=128))
