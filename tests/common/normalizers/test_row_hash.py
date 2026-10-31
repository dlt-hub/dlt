"""`get_row_hash` must not depend on the json backend or on the flavour of the value.

A pipeline hashes PUA-encoded strings, never these types, so this pins the function for any other
caller. The digests are hardcoded: only a fixed value catches a drift that duplicates merged rows.
"""

import datetime  # noqa: I251
import decimal
from typing import Any, Dict
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from dlt.common import json as dlt_json, pendulum
from dlt.common.json import _orjson, _simplejson
from dlt.common.libs.hexbytes import HexBytes
from dlt.common.normalizers.json import helpers
from dlt.common.normalizers.json.helpers import get_row_hash
from dlt.common.wei import Wei

JSON_IMPLS = {"orjson": _orjson, "simplejson": _simplejson}

BERLIN = ZoneInfo("Europe/Berlin")

# every data type dlt can put in a row, in both stdlib and pendulum flavours where both exist
ALL_TYPES: Dict[str, Any] = {
    "text": "a string",
    "bigint": 42,
    "double": 1.5,
    "bool": True,
    "none": None,
    "decimal": decimal.Decimal("1.25"),
    "wei": Wei("1.25"),
    "uuid": UUID("11111111-2222-3333-4444-555555555555"),
    "binary": b"\x00\x01\xfe",
    "hexbytes": HexBytes(b"\x00\x01"),
    "json": {"nested": [1, {"deep": "value"}]},
    "ts_utc_stdlib": datetime.datetime(2024, 1, 15, 23, 30, tzinfo=datetime.timezone.utc),
    "ts_utc_pendulum": pendulum.DateTime(2024, 1, 15, 23, 30, tzinfo=pendulum.UTC),
    "ts_offset": datetime.datetime(2024, 1, 15, 23, 30, tzinfo=BERLIN),
    "ts_naive": datetime.datetime(2024, 1, 15, 23, 30),
    "ts_micros": datetime.datetime(2024, 1, 15, 23, 30, 0, 123456, tzinfo=datetime.timezone.utc),
    "date_stdlib": datetime.date(2024, 1, 15),
    "date_pendulum": pendulum.Date(2024, 1, 15),
    "time_stdlib": datetime.time(23, 30, 15),
    "time_pendulum": pendulum.Time(23, 30, 15),
}

EXPECTED_ROW_HASH = "eONA4BD7501UNg"
EXPECTED_KEY_HASH = "hEHaRD9bb451fg"
KEY_SUBSET = ["bigint", "ts_utc_stdlib", "date_stdlib"]


@pytest.fixture(params=list(JSON_IMPLS), autouse=False)
def json_impl(request: Any, monkeypatch: pytest.MonkeyPatch) -> str:
    """Runs the test body against one json implementation."""
    monkeypatch.setattr(helpers, "json", JSON_IMPLS[request.param])
    return str(request.param)


def test_row_hash_does_not_depend_on_json_impl(json_impl: str) -> None:
    """orjson and simplejson must agree: the hash is row identity, not an encoding artifact."""
    assert get_row_hash(ALL_TYPES) == EXPECTED_ROW_HASH, json_impl


def test_key_hash_does_not_depend_on_json_impl(json_impl: str) -> None:
    assert get_row_hash(ALL_TYPES, subset=KEY_SUBSET) == EXPECTED_KEY_HASH, json_impl


@pytest.mark.parametrize(
    "stdlib_key,pendulum_key",
    [
        ("ts_utc_stdlib", "ts_utc_pendulum"),
        ("date_stdlib", "date_pendulum"),
        ("time_stdlib", "time_pendulum"),
    ],
)
def test_stdlib_and_pendulum_hash_alike(json_impl: str, stdlib_key: str, pendulum_key: str) -> None:
    """The same moment must hash the same whether it arrives as stdlib or pendulum."""
    assert get_row_hash({"v": ALL_TYPES[stdlib_key]}) == get_row_hash(
        {"v": ALL_TYPES[pendulum_key]}
    ), json_impl


def test_dlt_system_columns_are_excluded(json_impl: str) -> None:
    with_system = dict(ALL_TYPES, _dlt_id="x", _dlt_load_id="y", _dlt_parent_id="z")
    assert get_row_hash(with_system) == EXPECTED_ROW_HASH, json_impl


def test_column_order_does_not_matter(json_impl: str) -> None:
    reversed_row = dict(reversed(list(ALL_TYPES.items())))
    assert get_row_hash(reversed_row) == EXPECTED_ROW_HASH, json_impl


def test_subset_must_be_present() -> None:
    with pytest.raises(KeyError):
        get_row_hash(ALL_TYPES, subset=["no_such_column"])


@pytest.mark.parametrize("impl_name", list(JSON_IMPLS))
def test_utc_timestamp_renders_alike(impl_name: str) -> None:
    """The `Z` / `+00:00` choice must be one choice, or the hashes above cannot hold."""
    impl = JSON_IMPLS[impl_name]
    utc_stdlib = impl.dumps({"v": ALL_TYPES["ts_utc_stdlib"]})
    utc_pendulum = impl.dumps({"v": ALL_TYPES["ts_utc_pendulum"]})
    assert utc_stdlib == utc_pendulum, impl_name
