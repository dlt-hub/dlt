from typing import Dict, List, Any, Type

from pydantic import BaseModel

from dlt.extract.utils import ensure_table_schema_columns_hint
from dlt.common.schema.typing import TColumnSchema
from tests.cases import TABLE_UPDATE


def test_column_schema_from_list() -> None:
    result = ensure_table_schema_columns_hint(TABLE_UPDATE)

    for col in TABLE_UPDATE:
        assert result[col["name"]] == col  # type: ignore[index]


def test_dynamic_columns_schema_from_list() -> None:
    def dynamic_columns(item: Dict[str, Any]) -> List[TColumnSchema]:
        return TABLE_UPDATE

    result_func = ensure_table_schema_columns_hint(dynamic_columns)

    result = result_func({})  # type: ignore[operator]

    for col in TABLE_UPDATE:
        assert result[col["name"]] == col


def test_dynamic_columns_schema_from_pydantic() -> None:
    class Model(BaseModel):
        a: int
        b: str

    def dynamic_columns(item: Dict[str, Any]) -> Type[BaseModel]:
        return Model

    result_func = ensure_table_schema_columns_hint(dynamic_columns)

    result = result_func({})  # type: ignore[operator]

    assert result["a"]["data_type"] == "bigint"
    assert result["b"]["data_type"] == "text"
