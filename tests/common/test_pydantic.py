import pytest
from typing import Union, Optional, List, Dict

from datetime import datetime, date, time  # noqa: I251
from dlt.common import Decimal

from pydantic import BaseModel
from dlt.common.libs.pydantic import pydantic_to_table_schema_columns


class NestedModel(BaseModel):
    nested_field: str


class Model(BaseModel):
    bigint_field: int
    text_field: str
    timestamp_field: datetime
    date_field: date
    decimal_field: Decimal
    double_field: float
    time_field: time

    nested_field: NestedModel
    list_field: List[str]

    union_field: Union[int, str]

    optional_field: Optional[float]

    blank_dict_field: dict  # type: ignore[type-arg]
    parametrized_dict_field: Dict[str, int]


@pytest.mark.parametrize('instance', [True, False])
def test_pydantic_model_to_columns(instance: bool) -> None:
    if instance:
        model = Model(
            bigint_field=1, text_field="text", timestamp_field=datetime.now(),
            date_field=date.today(), decimal_field=Decimal(1.1), double_field=1.1,
            time_field=time(1, 2, 3, 12345),
            nested_field=NestedModel(nested_field="nested"),
            list_field=["a", "b", "c"],
            union_field=1,
            optional_field=None,
            blank_dict_field={},
            parametrized_dict_field={"a": 1, "b": 2, "c": 3}
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
    assert result["time_field"]["data_type"] == "time"
    assert result["nested_field"]["data_type"] == "complex"
    assert result['list_field']['data_type'] == 'complex'
    assert result['union_field']['data_type'] == 'bigint'
    assert result['optional_field']['data_type'] == 'double'
    assert result['optional_field']['nullable'] is True
    assert result['blank_dict_field']['data_type'] == 'complex'
    assert result['parametrized_dict_field']['data_type'] == 'complex'


def test_pydantic_model_skip_complex_types() -> None:
    result = pydantic_to_table_schema_columns(Model, skip_complex_types=True)

    assert result["bigint_field"]["data_type"] == "bigint"

    assert "nested_field" not in result
    assert "list_field" not in result
    assert "blank_dict_field" not in result
    assert "parametrized_dict_field" not in result
    assert result["bigint_field"]["data_type"] == "bigint"
    assert result["text_field"]["data_type"] == "text"
    assert result["timestamp_field"]["data_type"] == "timestamp"

