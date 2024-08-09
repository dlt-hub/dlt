import io
import os
from typing import List, NamedTuple
from dataclasses import dataclass
import pytest

from dlt.common import json, Decimal, pendulum
from dlt.common.arithmetics import numeric_default_context
from dlt.common import known_env
from dlt.common.json import (
    _DECIMAL,
    _WEI,
    PUA_START,
    PUA_CHARACTER_MAX,
    custom_pua_decode,
    may_have_pua,
    _orjson,
    _simplejson,
    SupportsJson,
    _DATETIME,
)

from tests.utils import autouse_test_storage, TEST_STORAGE_ROOT, preserve_environ
from tests.cases import (
    JSON_TYPED_DICT,
    JSON_TYPED_DICT_DECODED,
    JSON_TYPED_DICT_NESTED,
    JSON_TYPED_DICT_NESTED_DECODED,
)
from tests.common.utils import json_case_path, load_json_case


class NamedTupleTest(NamedTuple):
    str_field: str
    dec_field: Decimal


@dataclass
class DataClassTest:
    str_field: str
    int_field: int = 5
    dec_field: Decimal = Decimal("0.5")


_JSON_IMPL: List[SupportsJson] = [_orjson, _simplejson]  # type: ignore[list-item]


def test_orjson_default_imported() -> None:
    assert json._impl_name == "orjson"


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_load_save_binary(json_impl: SupportsJson) -> None:
    with open(json_case_path("weird_rows"), "rb") as f:
        doc = json_impl.load(f)
    assert doc[1]["str"] == "イロハニホヘト チリヌルヲ 'ワカヨタレソ ツネナラム"
    test_path = os.path.join(TEST_STORAGE_ROOT, "weird_rows_b.json")
    with open(test_path, "wb") as f:
        json_impl.dump(doc, f)
    saved_doc = load_json_case("weird_rows")
    with open(test_path, "rb") as f:
        dumped_doc = json_impl.load(f)
    assert doc == saved_doc == dumped_doc


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_string_serialization(json_impl: SupportsJson) -> None:
    with open(json_case_path("weird_rows"), "r", encoding="utf-8") as f:
        content = f.read()
    doc = json_impl.loads(content)
    serialized = json_impl.dumps(doc)
    assert "\n" not in serialized
    doc2 = json_impl.loads(serialized)
    assert doc == doc2
    assert doc[1]["str"] == "イロハニホヘト チリヌルヲ 'ワカヨタレソ ツネナラム"
    assert doc[3]["str"] == "hello\nworld\t\t\t\r\u0006"


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_bytes_serialization(json_impl: SupportsJson) -> None:
    with open(json_case_path("weird_rows"), "rb") as f:
        content = f.read()
    doc = json_impl.loadb(content)
    serialized = json_impl.dumpb(doc)
    assert b"\n" not in serialized
    doc2 = json_impl.loadb(serialized)
    assert doc == doc2
    assert doc[1]["str"] == "イロハニホヘト チリヌルヲ 'ワカヨタレソ ツネナラム"
    assert doc[3]["str"] == "hello\nworld\t\t\t\r\u0006"


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_pretty_print(json_impl: SupportsJson) -> None:
    # do pretty dump and read
    with open(json_case_path("weird_rows"), "rb") as f:
        doc = json_impl.load(f)
    test_path = os.path.join(TEST_STORAGE_ROOT, "weird_rows_b.json")
    with open(test_path, "wb") as f:
        json_impl.dump(doc, f, pretty=True)
    with open(test_path, "r", encoding="utf-8") as f:
        dumped_content = f.readlines()
    # dumped doc should have idents
    # print("".join(dumped_content))
    assert len(dumped_content) == 22

    # dumps
    content = json_impl.dumps(doc, pretty=True)
    assert len(content.splitlines()) == 22
    content_b = json_impl.dumpb(doc, pretty=True)
    assert len(content_b.splitlines()) == 22


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_order_keys(json_impl: SupportsJson) -> None:
    with open(json_case_path("mod_bot_case"), "rb") as f:
        doc = json_impl.load(f)
    # get keys in current order
    doc_keys = list(doc.keys())
    doc__data_keys = list(doc["data"].keys())

    test_path = os.path.join(TEST_STORAGE_ROOT, "mod_bot_case_b.json")
    with open(test_path, "wb") as f:
        json_impl.dump(doc, f, pretty=True, sort_keys=True)
    with open(test_path, "r", encoding="utf-8") as f:
        dumped_doc = json_impl.load(f)
    # keys are ordered
    assert list(dumped_doc.keys()) == list(sorted(doc_keys))
    assert list(dumped_doc["data"].keys()) == list(sorted(doc__data_keys))
    # and not ordered
    with open(test_path, "wb") as f:
        json_impl.dump(doc, f, pretty=True)
    with open(test_path, "r", encoding="utf-8") as f:
        dumped_doc = json_impl.load(f)
    assert list(dumped_doc.keys()) == doc_keys
    assert list(dumped_doc["data"].keys()) == doc__data_keys

    # also for strings
    content = json_impl.dumps(doc, sort_keys=True)
    dumped_doc = json_impl.loads(content)
    assert list(dumped_doc.keys()) == list(sorted(doc_keys))
    assert list(dumped_doc["data"].keys()) == list(sorted(doc__data_keys))
    content = json_impl.dumps(doc)
    dumped_doc = json_impl.loads(content)
    assert list(dumped_doc.keys()) == doc_keys
    assert list(dumped_doc["data"].keys()) == doc__data_keys

    content_b = json_impl.dumpb(doc, sort_keys=True)
    dumped_doc = json_impl.loadb(content_b)
    assert list(dumped_doc.keys()) == list(sorted(doc_keys))
    assert list(dumped_doc["data"].keys()) == list(sorted(doc__data_keys))
    content_b = json_impl.dumpb(doc)
    dumped_doc = json_impl.loadb(content_b)
    assert list(dumped_doc.keys()) == doc_keys
    assert list(dumped_doc["data"].keys()) == doc__data_keys


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_json_decimals(json_impl: SupportsJson) -> None:
    # deserialize as float
    d = json_impl.loads('{"v": 21.37}')
    assert type(d["v"]) is float

    # serialize as string
    s = json_impl.dumps({"decimal": Decimal("21.37")})
    assert s == '{"decimal":"21.37"}'

    # serialize max precision which is 10**38
    with numeric_default_context():
        s = json_impl.dumps({"decimal": Decimal(10**29) - Decimal("0.001")})
        assert s == '{"decimal":"99999999999999999999999999999.999"}'

    # serialize untypical context
    with numeric_default_context(precision=77):
        doc = {"decimal": Decimal(10**74) - Decimal("0.001")}
    # serialize out of local context
    s = json_impl.dumps(doc)
    # full precision. you need to quantize yourself if you need it
    assert (
        s
        == '{"decimal":"99999999999999999999999999999999999999999999999999999999999999999999999999.999"}'
    )


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_json_large_int(json_impl: SupportsJson):
    # optimized json parsers like orjson do not support large integers
    if json_impl._impl_name == "orjson":
        with pytest.raises(TypeError):
            doc = json_impl.dumps({"a": 2**256})
    else:
        doc = json_impl.dumps({"a": 2**256})
        assert json_impl.loads(doc)["a"] == 2**256


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_json_pendulum(json_impl: SupportsJson) -> None:
    dt_str = "2005-04-02T20:37:37.358236+00:00"
    r = json_impl.loads('{"t": "%s"}' % dt_str)
    # iso datetime string not deserialized to dates automatically
    assert r["t"] == dt_str
    # use zulu notation for UTC
    now = pendulum.parse("2005-04-02T20:37:37.358236Z")
    s = json_impl.dumps({"t": now})
    # must serialize UTC timezone
    assert s.endswith('+00:00"}')
    s_r = json_impl.loads(s)
    assert pendulum.parse(s_r["t"]) == now
    # serialize date and time
    s = json_impl.dumps(JSON_TYPED_DICT)
    s_r = json_impl.loads(s)
    assert s_r["date"] == "2022-02-02"
    assert s_r["time"] == "20:37:37.358236"
    # Decodes zulu notation as well
    dt_str_z = "2005-04-02T20:37:37.358236Z"
    s = f'"{_DATETIME + dt_str_z}"'
    s_r = json_impl.typed_loads(s)
    assert s_r == pendulum.parse(dt_str_z)


# @pytest.mark.parametrize("json_impl", _JSON_IMPL)
# def test_json_timedelta(json_impl: SupportsJson) -> None:
#     from datetime import timedelta
#     start_date = pendulum.parse("2005-04-02T20:37:37.358236Z")
#     delta = pendulum.interval(start_date, pendulum.now())
#     assert isinstance(delta, timedelta)
#     print(str(delta.as_timedelta()))


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_json_named_tuple(json_impl: SupportsJson) -> None:
    assert (
        json_impl.dumps(NamedTupleTest("STR", Decimal("1.3333")))
        == '{"str_field":"STR","dec_field":"1.3333"}'
    )
    with io.BytesIO() as b:
        json_impl.typed_dump(NamedTupleTest("STR", Decimal("1.3333")), b)
        assert (
            b.getvalue().decode("utf-8") == '{"str_field":"STR","dec_field":"%s1.3333"}' % _DECIMAL
        )


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_data_class(json_impl: SupportsJson) -> None:
    assert (
        json_impl.dumps(DataClassTest(str_field="AAA"))
        == '{"str_field":"AAA","int_field":5,"dec_field":"0.5"}'
    )
    with io.BytesIO() as b:
        json_impl.typed_dump(DataClassTest(str_field="AAA"), b)
        assert (
            b.getvalue().decode("utf-8")
            == '{"str_field":"AAA","int_field":5,"dec_field":"%s0.5"}' % _DECIMAL
        )


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_json_encode(json_impl: SupportsJson) -> None:
    j = json_impl.dumps(JSON_TYPED_DICT)
    d = json_impl.loads(j)
    # all our values are strings
    assert all(isinstance(v, str) for v in d.values())


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_json_typed_dumps_loads(json_impl: SupportsJson) -> None:
    s = json_impl.typed_dumps(JSON_TYPED_DICT_NESTED)
    b = json_impl.typed_dumpb(JSON_TYPED_DICT_NESTED)

    assert json_impl.typed_loads(s) == JSON_TYPED_DICT_NESTED_DECODED
    assert json_impl.typed_loadb(b) == JSON_TYPED_DICT_NESTED_DECODED

    # Test load/dump naked
    dt = pendulum.now()
    s = json_impl.typed_dumps(dt)
    assert json_impl.typed_loads(s) == dt


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_json_typed_encode(json_impl: SupportsJson) -> None:
    with io.BytesIO() as b:
        json_impl.typed_dump(JSON_TYPED_DICT, b)
        j = b.getvalue().decode("utf-8")
    # use normal decoder
    d = json_impl.loads(j)
    # we have pua chars
    assert d["decimal"][0] == _DECIMAL
    assert d["wei"][0] == _WEI
    # decode all
    d_d = {k: custom_pua_decode(v) for k, v in d.items()}
    assert d_d == JSON_TYPED_DICT_DECODED


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_pua_detection(json_impl: SupportsJson) -> None:
    with io.BytesIO() as b:
        json_impl.typed_dump(JSON_TYPED_DICT, b)
        content_b = b.getvalue()
    assert may_have_pua(content_b)
    with open(json_case_path("rasa_event_bot_metadata"), "rb") as f:
        content_b = f.read()
    assert not may_have_pua(content_b)


@pytest.mark.parametrize("json_impl", _JSON_IMPL)
def test_garbage_pua_string(json_impl: SupportsJson) -> None:
    for c in range(PUA_START, PUA_START + PUA_CHARACTER_MAX):
        garbage = f"{chr(c)}GABAGE🤷"
        with io.BytesIO() as b:
            json_impl.typed_dump(garbage, b)
            content_b = b.getvalue()
        assert may_have_pua(content_b)
        # if cannot parse, the initial object is returned
        assert custom_pua_decode(json_impl.loadb(content_b)) == garbage


def test_change_pua_start() -> None:
    import inspect

    os.environ[known_env.DLT_JSON_TYPED_PUA_START] = "0x0FA179"
    from importlib import reload

    try:
        reload(inspect.getmodule(SupportsJson))
        from dlt.common.json import PUA_START as MOD_PUA_START

        assert MOD_PUA_START == int("0x0FA179", 16)
    finally:
        # restore old start
        os.environ[known_env.DLT_JSON_TYPED_PUA_START] = hex(PUA_START)
        from importlib import reload

        reload(inspect.getmodule(SupportsJson))
        from dlt.common.json import PUA_START as MOD_PUA_START

        assert MOD_PUA_START == PUA_START


def test_load_and_compare_all_impls() -> None:
    with open(json_case_path("rasa_event_bot_metadata"), "rb") as f:
        content_b = f.read()

    docs = [impl.loadb(content_b) for impl in _JSON_IMPL]
    dump_s = [impl.dumps(docs[idx]) for idx, impl in enumerate(_JSON_IMPL)]
    dump_b = [impl.dumpb(docs[idx]) for idx, impl in enumerate(_JSON_IMPL)]

    # same docs, same output
    for idx in range(0, len(docs) - 1):
        assert docs[idx] == docs[idx + 1]
        assert dump_s[idx] == dump_s[idx + 1]
        assert dump_b[idx] == dump_b[idx + 1]
