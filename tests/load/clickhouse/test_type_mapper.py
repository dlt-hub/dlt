import pytest
from dlt.destinations.impl.clickhouse.factory import ClickHouseTypeMapper

@pytest.mark.parametrize(
    "precision,expected_type",
    [
        (None, "Int64"),
        (8, "Int8"),
        (16, "Int16"),
        (32, "Int32"),
        (64, "Int64"),
    ]
)
def test_to_db_integer_type_valid(precision, expected_type):
    mapper = ClickHouseTypeMapper(capabilities=None)
    column = {"precision": precision} if precision is not None else {}
    assert mapper.to_db_integer_type(column) == expected_type

def test_to_db_integer_type_invalid():
    mapper = ClickHouseTypeMapper(capabilities=None)
    with pytest.raises(ValueError):
        mapper.to_db_integer_type({"precision": 128})
