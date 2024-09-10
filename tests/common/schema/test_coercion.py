from collections.abc import Mapping, MutableSequence
from copy import copy
from typing import Any, Type
import pytest
import datetime  # noqa: I251
from hexbytes import HexBytes
from enum import Enum

from pendulum.tz import UTC

from dlt.common import Decimal, Wei, json, pendulum
from dlt.common.json import _DATETIME, custom_pua_decode_nested
from dlt.common.data_types import coerce_value, py_type_to_sc_type, TDataType

from tests.cases import JSON_TYPED_DICT, JSON_TYPED_DICT_TYPES


def test_coerce_same_type() -> None:
    # same type coercion
    assert coerce_value("double", "double", 8721.1) == 8721.1
    assert coerce_value("bigint", "bigint", 8721) == 8721
    # try various special types
    for k, v in JSON_TYPED_DICT.items():
        typ_ = JSON_TYPED_DICT_TYPES[k]
        assert coerce_value(typ_, typ_, v)


def test_coerce_type_to_text() -> None:
    assert coerce_value("text", "bool", False) == str(False)
    # double into text
    assert coerce_value("text", "double", -1726.1288) == "-1726.1288"
    # bytes to text (base64)
    assert coerce_value("text", "binary", b"binary string") == "YmluYXJ5IHN0cmluZw=="
    # HexBytes to text (hex with prefix)
    assert (
        coerce_value("text", "binary", HexBytes(b"binary string")) == "0x62696e61727920737472696e67"
    )

    # Str enum value
    class StrEnum(Enum):
        a = "a_value"
        b = "b_value"

    str_enum_result = coerce_value("text", "text", StrEnum.b)
    # Make sure we get the bare str value, not the enum instance
    assert not isinstance(str_enum_result, Enum)
    assert str_enum_result == "b_value"

    # Mixed enum value
    class MixedEnum(Enum):
        a = "a_value"
        b = 1

    mixed_enum_result = coerce_value("text", "text", MixedEnum.b)
    # Make sure we get the bare str value, not the enum instance
    assert not isinstance(mixed_enum_result, Enum)
    assert mixed_enum_result == "1"


def test_coerce_type_to_bool() -> None:
    # text into bool
    assert coerce_value("bool", "text", "False") is False
    assert coerce_value("bool", "text", "yes") is True
    assert coerce_value("bool", "text", "no") is False
    # some numeric types
    assert coerce_value("bool", "bigint", 1) is True
    assert coerce_value("bool", "bigint", 0) is False
    assert coerce_value("bool", "decimal", Decimal(1)) is True
    assert coerce_value("bool", "decimal", Decimal(0)) is False

    # no coercions
    with pytest.raises(ValueError):
        coerce_value("bool", "json", {"a": True})
    with pytest.raises(ValueError):
        coerce_value("bool", "binary", b"True")
    with pytest.raises(ValueError):
        coerce_value("bool", "timestamp", pendulum.now())


def test_coerce_type_to_double() -> None:
    # bigint into double
    assert coerce_value("double", "bigint", 762162) == 762162.0
    # text into double if parsable
    assert coerce_value("double", "text", " -1726.1288 ") == -1726.1288
    # hex text into double
    assert coerce_value("double", "text", "0xff") == 255.0
    # wei, decimal to double
    assert coerce_value("double", "wei", Wei.from_int256(2137, decimals=2)) == 21.37
    assert coerce_value("double", "decimal", Decimal("-1121.11")) == -1121.11
    # non parsable text
    with pytest.raises(ValueError):
        coerce_value("double", "text", "a912.12")
    # bool does not coerce
    with pytest.raises(ValueError):
        coerce_value("double", "bool", False)


def test_coerce_type_to_bigint() -> None:
    assert coerce_value("bigint", "text", " -1726 ") == -1726
    # for round numerics we can convert
    assert coerce_value("bigint", "double", -762162.0) == -762162
    assert coerce_value("bigint", "decimal", Decimal("1276.0")) == 1276
    assert coerce_value("bigint", "wei", Wei("1276.0")) == 1276

    # raises when not round
    with pytest.raises(ValueError):
        coerce_value("bigint", "double", 762162.1)
    with pytest.raises(ValueError):
        coerce_value("bigint", "decimal", Decimal(912.12))
    with pytest.raises(ValueError):
        coerce_value("bigint", "wei", Wei(912.12))

    # non parsable floats and ints
    with pytest.raises(ValueError):
        coerce_value("bigint", "text", "f912.12")
    with pytest.raises(ValueError):
        coerce_value("bigint", "text", "912.12")

    # Int enum value
    class IntEnum(int, Enum):
        a = 1
        b = 2

    int_enum_result = coerce_value("bigint", "bigint", IntEnum.b)
    # Make sure we get the bare int value, not the enum instance
    assert not isinstance(int_enum_result, Enum)
    assert int_enum_result == 2


@pytest.mark.parametrize("dec_cls,data_type", [(Decimal, "decimal"), (Wei, "wei")])
def test_coerce_to_numeric(dec_cls: Type[Any], data_type: TDataType) -> None:
    v = coerce_value(data_type, "text", " -1726.839283 ")
    assert type(v) is dec_cls
    assert v == dec_cls("-1726.839283")
    v = coerce_value(data_type, "bigint", -1726)
    assert type(v) is dec_cls
    assert v == dec_cls("-1726")
    # mind that 1276.37 does not have binary representation as used in float
    v = coerce_value(data_type, "double", 1276.37)
    assert type(v) is dec_cls
    assert v.quantize(Decimal("1.00")) == dec_cls("1276.37")

    # wei to decimal and reverse
    v = coerce_value(data_type, "decimal", Decimal("1276.37"))
    assert type(v) is dec_cls
    assert v == dec_cls("1276.37")
    v = coerce_value(data_type, "wei", Wei("1276.37"))
    assert type(v) is dec_cls
    assert v == dec_cls("1276.37")

    # invalid format
    with pytest.raises(ValueError):
        coerce_value(data_type, "text", "p912.12")


def test_coerce_type_from_hex_text() -> None:
    # hex text into various types
    assert coerce_value("wei", "text", " 0xff") == 255
    assert coerce_value("bigint", "text", " 0xff") == 255
    assert coerce_value("decimal", "text", " 0xff") == Decimal(255)
    assert coerce_value("double", "text", " 0xff") == 255.0


def test_coerce_type_to_timestamp() -> None:
    # timestamp cases
    assert coerce_value("timestamp", "text", " 1580405246 ") == pendulum.parse(
        "2020-01-30T17:27:26+00:00"
    )
    # the tenths of microseconds will be ignored
    assert coerce_value("timestamp", "double", 1633344898.7415245) == pendulum.parse(
        "2021-10-04T10:54:58.741524+00:00"
    )
    # if text is ISO string it will be coerced
    assert coerce_value("timestamp", "text", "2022-05-10T03:41:31.466000+00:00") == pendulum.parse(
        "2022-05-10T03:41:31.466000+00:00"
    )
    assert coerce_value("timestamp", "text", "2022-05-10T03:41:31.466+02:00") == pendulum.parse(
        "2022-05-10T01:41:31.466Z"
    )
    assert coerce_value("timestamp", "text", "2022-05-10T03:41:31.466+0200") == pendulum.parse(
        "2022-05-10T01:41:31.466Z"
    )
    # parse almost ISO compliant string
    assert coerce_value("timestamp", "text", "2022-04-26 10:36+02") == pendulum.parse(
        "2022-04-26T10:36:00+02:00"
    )
    assert coerce_value("timestamp", "text", "2022-04-26 10:36") == pendulum.parse(
        "2022-04-26T10:36:00+00:00"
    )
    # parse date string
    assert coerce_value("timestamp", "text", "2021-04-25") == pendulum.parse("2021-04-25")
    # from date type
    assert coerce_value("timestamp", "date", datetime.date(2023, 2, 27)) == pendulum.parse(
        "2023-02-27"
    )

    # fails on "now" - yes pendulum by default parses "now" as .now()
    with pytest.raises(ValueError):
        coerce_value("timestamp", "text", "now")

    # fails on intervals - pendulum by default parses a string into: datetime, data, time or interval
    with pytest.raises(ValueError):
        coerce_value("timestamp", "text", "2007-03-01T13:00:00Z/2008-05-11T15:30:00Z")
    with pytest.raises(ValueError):
        coerce_value("timestamp", "text", "2011--20012")

    # test wrong unix timestamps
    with pytest.raises(ValueError):
        coerce_value("timestamp", "double", -1000000000000000000000000000)
    with pytest.raises(ValueError):
        coerce_value("timestamp", "double", 1000000000000000000000000000)

    # formats with timezones are not parsed
    with pytest.raises(ValueError):
        coerce_value("timestamp", "text", "06/04/22, 11:15PM IST")

    # we do not parse RFC 822, 2822, 850 etc.
    with pytest.raises(ValueError):
        coerce_value("timestamp", "text", "Wed, 06 Jul 2022 11:58:08 +0200")
    with pytest.raises(ValueError):
        coerce_value("timestamp", "text", "Tuesday, 13-Sep-22 18:42:31 UTC")

    # time data type not supported yet
    with pytest.raises(ValueError):
        coerce_value("timestamp", "text", "10:36")

    # non parsable datetime
    with pytest.raises(ValueError):
        coerce_value("timestamp", "text", "x2022-05-10T03:41:31.466000X+00:00")

    # iso time string fails
    with pytest.raises(ValueError):
        coerce_value("timestamp", "text", "03:41:31.466")

    # time object fails
    with pytest.raises(TypeError):
        coerce_value("timestamp", "time", pendulum.Time(10, 36, 0, 0))


def test_coerce_type_to_date() -> None:
    # from datetime object
    assert coerce_value(
        "date", "timestamp", pendulum.datetime(1995, 5, 6, 00, 1, 1, tz=UTC)
    ) == pendulum.parse("1995-05-06", exact=True)
    # from unix timestamp
    assert coerce_value("date", "double", 1677546399.494264) == pendulum.parse(
        "2023-02-28", exact=True
    )
    assert coerce_value("date", "text", " 1677546399 ") == pendulum.parse("2023-02-28", exact=True)
    # ISO date string
    assert coerce_value("date", "text", "2023-02-27") == pendulum.parse("2023-02-27", exact=True)
    # ISO datetime string
    assert coerce_value("date", "text", "2022-05-10T03:41:31.466000+00:00") == pendulum.parse(
        "2022-05-10", exact=True
    )
    assert coerce_value("date", "text", "2022-05-10T03:41:31.466+02:00") == pendulum.parse(
        "2022-05-10", exact=True
    )
    assert coerce_value("date", "text", "2022-05-10T03:41:31.466+0200") == pendulum.parse(
        "2022-05-10", exact=True
    )
    # almost ISO compliant string
    assert coerce_value("date", "text", "2022-04-26 10:36+02") == pendulum.parse(
        "2022-04-26", exact=True
    )
    assert coerce_value("date", "text", "2022-04-26 10:36") == pendulum.parse(
        "2022-04-26", exact=True
    )

    # iso time string fails
    with pytest.raises(ValueError):
        coerce_value("timestamp", "text", "03:41:31.466")

    # time object fails
    with pytest.raises(TypeError):
        coerce_value("timestamp", "time", pendulum.Time(10, 36, 0, 0))


def test_coerce_type_to_time() -> None:
    # from ISO time string
    assert coerce_value("time", "text", "03:41:31.466000") == pendulum.parse(
        "03:41:31.466000", exact=True
    )
    # time object returns same value
    assert coerce_value("time", "time", pendulum.time(3, 41, 31, 466000)) == pendulum.time(
        3, 41, 31, 466000
    )
    # from datetime object fails
    with pytest.raises(TypeError):
        coerce_value("time", "timestamp", pendulum.datetime(1995, 5, 6, 00, 1, 1, tz=UTC))

    # from unix timestamp fails
    with pytest.raises(TypeError):
        assert coerce_value("time", "double", 1677546399.494264) == pendulum.parse(
            "01:06:39.494264", exact=True
        )
    with pytest.raises(ValueError):
        assert coerce_value("time", "text", " 1677546399 ") == pendulum.parse(
            "01:06:39", exact=True
        )
    # ISO date string fails
    with pytest.raises(ValueError):
        assert coerce_value("time", "text", "2023-02-27") == pendulum.parse("00:00:00", exact=True)
    # ISO datetime string fails
    with pytest.raises(ValueError):
        assert coerce_value("time", "text", "2022-05-10T03:41:31.466000+00:00")


def test_coerce_type_to_binary() -> None:
    # from hex string
    assert coerce_value("binary", "text", "0x30") == b"0"
    # from base64
    assert coerce_value("binary", "text", "YmluYXJ5IHN0cmluZw==") == b"binary string"
    # int into bytes
    assert coerce_value("binary", "bigint", 15) == b"\x0f"
    # can't into double
    with pytest.raises(ValueError):
        coerce_value("binary", "double", 912.12)
    # can't broken base64
    with pytest.raises(ValueError):
        assert coerce_value("binary", "text", "!YmluYXJ5IHN0cmluZw==")


def test_py_type_to_sc_type() -> None:
    assert py_type_to_sc_type(bool) == "bool"
    assert py_type_to_sc_type(int) == "bigint"
    assert py_type_to_sc_type(float) == "double"
    assert py_type_to_sc_type(str) == "text"
    assert py_type_to_sc_type(type(pendulum.now())) == "timestamp"
    assert py_type_to_sc_type(type(datetime.datetime(1988, 12, 1))) == "timestamp"
    assert py_type_to_sc_type(type(pendulum.date(2023, 2, 27))) == "date"
    assert py_type_to_sc_type(type(datetime.date.today())) == "date"
    assert py_type_to_sc_type(type(pendulum.time(1, 6, 39))) == "time"
    assert py_type_to_sc_type(type(Decimal(1))) == "decimal"
    assert py_type_to_sc_type(type(HexBytes("0xFF"))) == "binary"
    assert py_type_to_sc_type(type(Wei.from_int256(2137, decimals=2))) == "wei"
    # unknown types raise TypeException
    with pytest.raises(TypeError):
        py_type_to_sc_type(Any)  # type: ignore[arg-type]
    # none type raises TypeException
    with pytest.raises(TypeError):
        py_type_to_sc_type(type(None))
    # nested types
    assert py_type_to_sc_type(list) == "json"
    # assert py_type_to_sc_type(set) == "json"
    assert py_type_to_sc_type(dict) == "json"
    assert py_type_to_sc_type(tuple) == "json"
    assert py_type_to_sc_type(Mapping) == "json"
    assert py_type_to_sc_type(MutableSequence) == "json"

    class IntEnum(int, Enum):
        a = 1
        b = 2

    class StrEnum(str, Enum):
        a = "a"
        b = "b"

    class MixedEnum(Enum):
        a = 1
        b = "b"

    assert py_type_to_sc_type(IntEnum) == "bigint"
    assert py_type_to_sc_type(StrEnum) == "text"
    assert py_type_to_sc_type(MixedEnum) == "text"


def test_coerce_type_json() -> None:
    # dicts and lists should be coerced into strings automatically
    v_list = [1, 2, "3", {"json": True}]
    v_dict = {"list": [1, 2], "str": "json"}
    assert py_type_to_sc_type(type(v_list)) == "json"
    assert py_type_to_sc_type(type(v_dict)) == "json"
    assert type(coerce_value("json", "json", v_dict)) is dict
    assert type(coerce_value("json", "json", v_list)) is list
    assert coerce_value("json", "json", v_dict) == v_dict
    assert coerce_value("json", "json", v_list) == v_list
    assert coerce_value("text", "json", v_dict) == json.dumps(v_dict)
    assert coerce_value("text", "json", v_list) == json.dumps(v_list)
    assert coerce_value("json", "text", json.dumps(v_dict)) == v_dict
    assert coerce_value("json", "text", json.dumps(v_list)) == v_list

    # all other coercions fail
    with pytest.raises(ValueError):
        coerce_value("binary", "json", v_list)

    with pytest.raises(ValueError):
        coerce_value("json", "text", "not a json string")


def test_coerce_type_json_with_pua() -> None:
    v_dict = {
        "list": [1, Wei.from_int256(10**18), f"{_DATETIME}2022-05-10T01:41:31.466Z"],
        "str": "json",
        "pua_date": f"{_DATETIME}2022-05-10T01:41:31.466Z",
    }
    exp_v = {
        "list": [1, Wei.from_int256(10**18), "2022-05-10T01:41:31.466Z"],
        "str": "json",
        "pua_date": "2022-05-10T01:41:31.466Z",
    }
    assert coerce_value("json", "json", copy(v_dict)) == exp_v
    assert coerce_value("text", "json", copy(v_dict)) == json.dumps(exp_v)

    # TODO: what to test for this case if at all?
    # assert coerce_value("json", "text", json.dumps(v_dict)) == exp_v

    # also decode recursively
    custom_pua_decode_nested(v_dict)
    # restores datetime type
    assert v_dict["pua_date"] == pendulum.parse("2022-05-10T01:41:31.466Z")
