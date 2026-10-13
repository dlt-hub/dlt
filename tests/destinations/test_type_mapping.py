from typing import cast

import pytest

from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema
from dlt.destinations import snowflake
from dlt.destinations.impl.snowflake.factory import SnowflakeTypeMapper


@pytest.mark.parametrize(
    "column,expected",
    [
        ({"data_type": "text", "precision": 50, "scale": 2}, "VARCHAR(50)"),
        ({"data_type": "time", "precision": 3, "scale": 2}, "TIME(3)"),
        ({"data_type": "decimal", "precision": 10, "scale": 3}, "NUMBER(10,3)"),
    ],
    ids=["text_stale_scale", "time_stale_scale", "decimal"],
)
def test_scale_used_only_for_decimal(column: TColumnSchema, expected: str) -> None:
    mapper = SnowflakeTypeMapper(snowflake()._raw_capabilities())
    column = cast(TColumnSchema, {"name": "col", **column})
    table = cast(PreparedTableSchema, {"name": "table", "columns": {"col": column}})
    assert mapper.to_destination_type(column, table) == expected
