import pytest
from typing import Union, Optional, List

from datetime import datetime, date  # noqa: I251
from dlt.common import Decimal

from pydantic import BaseModel
from dlt.common.pydantic import pydantic_to_table_schema_columns


@pytest.mark.parametrize('instance', [True, False])
def test_pydantic_model_to_columns(instance: bool) -> None:

    class NestedModel(BaseModel):
        nested_field: str


    class Model(BaseModel):
        bigint_field: int
        text_field: str
        timestamp_field: datetime
        date_field: date
        decimal_field: Decimal
        double_field: float

        nested_field: NestedModel
        list_field: List[str]

        union_field: Union[int, str]

        optional_field: Optional[float]

    if instance:
        model = Model(
            bigint_field=1, text_field="text", timestamp_field=datetime.now(),
            date_field=date.today(), decimal_field=Decimal(1.1), double_field=1.1,
            nested_field=NestedModel(nested_field="nested"),
            list_field=["a", "b", "c"],
            union_field=1,
            optional_field=None,
        )
    else:
        model = Model  # type: ignore[assignment]

    result = pydantic_to_table_schema_columns(model)

    assert result["bigint_field"]["data_type"] == "bigint"
    assert result["text_field"]["data_type"] == "text"
    assert result["timestamp_field"]["data_type"] == "timestamp"
    assert result["date_field"]["data_type"] == "date"
    assert result["decimal_field"]["data_type"] == "decimal"
    assert result["double_field"]["data_type"] == "double"
    assert result["nested_field"]["data_type"] == "complex"
    assert result['list_field']['data_type'] == 'complex'
    assert result['union_field']['data_type'] == 'bigint'
    assert result['optional_field']['data_type'] == 'double'
    assert result['optional_field']['nullable'] is True

    class Model2(BaseModel):
        bigint_field: int
        text_field: str
        timestamp_field: datetime
        date_field: date
        decimal_field: Decimal
        double_field: float

        nested_field: NestedModel
        list_field: List[str]

        union_field: Union[int, str]

        optional_field: Optional[float] = None
