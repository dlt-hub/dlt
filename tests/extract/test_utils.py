import datetime  # noqa: I251
import sys
from importlib.machinery import ModuleSpec
from types import FunctionType, ModuleType
from typing import Dict, List, Any, Type
import pytest
import pytz
from pydantic import BaseModel

from dlt.common.schema.typing import TColumnSchema
from dlt.common.libs.pyarrow import pyarrow as pa

from dlt.extract import utils as extract_utils
from dlt.extract.utils import ensure_table_schema_columns_hint
from dlt.extract.utils import ensure_table_schema_columns
from dlt.extract.utils import digest_dedup_value, resolve_column_value, get_code_ref
from dlt.extract.decorators import resource

from tests.cases import TABLE_UPDATE
from tests.common.normalizers.test_row_hash import ALL_TYPES, BERLIN, JSON_IMPLS, KEY_SUBSET

# the dedup hash runs over raw values through untyped `json.dumps`, datetimes as epoch microseconds,
# so a change in a backend's rendering or in the digest length moves these and replays rows on the
# cursor boundary
EXPECTED_DEDUP_HASH = "r74ru5MyM+cp"
EXPECTED_KEY_DEDUP_HASH = "YE7Zd+POmhwr"
EXPECTED_COMPOUND_KEY_DEDUP_HASH = "sRhsRYvbPQC3"
# produced by dlt 1.25.0 as `digest128(json.dumps(value, sort_keys=True))` on each backend, only
# orjson rendered UTC with `Z`
EXPECTED_LEGACY_DEDUP_HASH = {
    "orjson": "RTNR348/3bi3zOy1XNxx",
    "simplejson": "eONA4BD7501UNiqKLGha",
}
EXPECTED_LEGACY_COMPOUND_KEY_DEDUP_HASH = {
    "orjson": "R4Ri7f3AxOwy/W0NEp67",
    "simplejson": "cRKc7ZQZ6R8XzqRlIUpd",
}
EXPECTED_LEGACY_KEY_DEDUP_HASH = "YE7Zd+POmhwrvXqiP8to"


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


@pytest.fixture(params=list(JSON_IMPLS))
def json_impl(request: Any, monkeypatch: pytest.MonkeyPatch) -> str:
    """Runs the test body against one json implementation."""
    monkeypatch.setattr(extract_utils, "json", JSON_IMPLS[request.param])
    return str(request.param)


def test_dedup_hash_does_not_depend_on_json_impl(json_impl: str) -> None:
    """orjson and simplejson must agree: the hash is row identity kept in state across runs."""
    assert digest_dedup_value(ALL_TYPES) == EXPECTED_DEDUP_HASH, json_impl
    key = resolve_column_value("bigint", ALL_TYPES)
    assert digest_dedup_value(key) == EXPECTED_KEY_DEDUP_HASH, json_impl
    compound_key = resolve_column_value(KEY_SUBSET, ALL_TYPES)
    assert digest_dedup_value(compound_key) == EXPECTED_COMPOUND_KEY_DEDUP_HASH, json_impl


def test_legacy_dedup_hash_matches_each_backend(json_impl: str) -> None:
    """The legacy digest is what the same backend wrote before 1.29, not one shared form."""
    assert digest_dedup_value(ALL_TYPES, legacy=True) == EXPECTED_LEGACY_DEDUP_HASH[json_impl]
    compound_key = resolve_column_value(KEY_SUBSET, ALL_TYPES)
    assert (
        digest_dedup_value(compound_key, legacy=True)
        == EXPECTED_LEGACY_COMPOUND_KEY_DEDUP_HASH[json_impl]
    )
    # SHAKE-128 is extendable: without a UTC datetime the short hash is a prefix of the legacy one
    key_legacy = digest_dedup_value(resolve_column_value("bigint", ALL_TYPES), legacy=True)
    assert key_legacy == EXPECTED_LEGACY_KEY_DEDUP_HASH, json_impl
    assert key_legacy.startswith(EXPECTED_KEY_DEDUP_HASH)


@pytest.mark.parametrize(
    "stdlib_key,pendulum_key",
    [
        ("ts_utc_stdlib", "ts_utc_pendulum"),
        ("date_stdlib", "date_pendulum"),
        ("time_stdlib", "time_pendulum"),
    ],
)
def test_stdlib_and_pendulum_dedup_hash_alike(
    json_impl: str, stdlib_key: str, pendulum_key: str
) -> None:
    """The same moment must hash the same whether it arrives as stdlib or pendulum."""
    assert digest_dedup_value(ALL_TYPES[stdlib_key]) == digest_dedup_value(
        ALL_TYPES[pendulum_key]
    ), json_impl


@pytest.mark.parametrize(
    "value",
    [
        ALL_TYPES["ts_utc_pendulum"],
        datetime.datetime(2024, 1, 16, 0, 30, tzinfo=BERLIN),
        datetime.datetime(2024, 1, 15, 23, 30),
        pytz.UTC.localize(datetime.datetime(2024, 1, 15, 23, 30)),
    ],
    ids=["pendulum", "berlin", "naive", "pytz"],
)
def test_dedup_hash_pins_datetime_instant(json_impl: str, value: Any) -> None:
    """One instant hashes alike whatever zone or awareness it arrives with, nested values too."""
    utc = ALL_TYPES["ts_utc_stdlib"]
    expected = digest_dedup_value({"k": [utc, {"ts": utc}]})
    assert digest_dedup_value({"k": [value, {"ts": value}]}) == expected, json_impl


def _gen():
    yield 1


async def _agen():
    yield 1


class _Api:
    def items(self):
        yield 1


def test_get_code_ref() -> None:
    module = "tests.extract.test_utils"
    assert get_code_ref(_gen) == f"{module}._gen"
    # generators and async generators are named after their function
    assert get_code_ref(_gen()) == f"{module}._gen"
    assert get_code_ref(_agen()) == f"{module}._agen"
    assert get_code_ref(_Api().items()) == f"{module}._Api.items"

    def inner():
        yield 1

    assert get_code_ref(inner) == f"{module}.test_get_code_ref.<locals>.inner"

    # an exhausted generator has no frame to take the module from
    exhausted = _gen()
    list(exhausted)
    assert get_code_ref(exhausted) is None

    # anonymous code shares its name with other code in the module
    assert get_code_ref(lambda: [1]) is None
    assert get_code_ref(x for x in [1]) is None

    # data does not come from code that can be named
    assert get_code_ref([1]) is None
    assert get_code_ref(iter([1])) is None
    assert get_code_ref(_Api().items) is None


def test_get_code_ref_in_main_module(monkeypatch: pytest.MonkeyPatch) -> None:
    """Functions and generators of a module run with `python -m` are named after that module"""
    main = ModuleType("__main__")
    main.__spec__ = ModuleSpec("pipelines.shop", None)
    monkeypatch.setitem(sys.modules, "__main__", main)

    rows = FunctionType(_gen.__code__, {"__name__": "__main__"}, "rows")
    assert get_code_ref(rows) == f"pipelines.shop.{rows.__qualname__}"
    # a generator object takes its module from the frame globals, not from the function
    assert get_code_ref(rows()) == f"pipelines.shop.{rows().__qualname__}"

    # run as a script there is no module name to recover
    main.__spec__ = None
    assert get_code_ref(rows()) == f"__main__.{rows().__qualname__}"
