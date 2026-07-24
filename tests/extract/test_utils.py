from typing import Dict, List, Any, Type
from pydantic import BaseModel

from dlt.common.schema.typing import TColumnSchema
from dlt.common.libs.pyarrow import pyarrow as pa

from dlt.extract.utils import ensure_table_schema_columns_hint
from dlt.extract.utils import ensure_table_schema_columns
from dlt.extract.decorators import resource

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


def test_column_schema_from_arrow_schema() -> None:
    arrow_schema = pa.schema(
        [
            pa.field("i", pa.int64(), nullable=False),
            pa.field("s", pa.string()),
            pa.field("payload", pa.struct([("a", pa.int64()), ("b", pa.string())])),
        ]
    )
    result = ensure_table_schema_columns(arrow_schema)
    assert result["i"]["data_type"] == "bigint"
    assert result["i"]["nullable"] is False
    assert result["s"]["data_type"] == "text"
    # nested field becomes a `json` column carrying the arrow type
    assert result["payload"]["data_type"] == "json"
    assert "x-nested-type" in result["payload"]
    # an arrow schema is not callable, so the hint wrapper resolves it eagerly
    assert ensure_table_schema_columns_hint(arrow_schema) == result


def test_resource_columns_from_arrow_schema() -> None:
    # only `payload` is declared via the schema; `i` would be inferred from data
    arrow_schema = pa.schema([pa.field("payload", pa.struct([("a", pa.int64())]))])

    @resource(columns=arrow_schema)
    def r() -> Any:
        yield {"i": 1, "payload": {"a": 2}}

    cols = r.compute_table_schema()["columns"]
    assert cols["payload"]["data_type"] == "json"
    assert "x-nested-type" in cols["payload"]
