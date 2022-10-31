from collections.abc import Mapping, MutableSequence
from typing import Any, Type
import pytest
import datetime  # noqa: I251
from hexbytes import HexBytes

from dlt.common import Decimal, Wei, json, pendulum
from dlt.common.schema import utils
from dlt.common.schema.typing import TDataType


from tests.cases import JSON_TYPED_DICT, JSON_TYPED_DICT_TYPES


def test_coerce_same_type() -> None:
    # same type coercion
    assert utils.coerce_type("double", "double", 8721.1) == 8721.1
    assert utils.coerce_type("bigint", "bigint", 8721) == 8721
    # try various special types
    for k, v in JSON_TYPED_DICT.items():
        typ_ = JSON_TYPED_DICT_TYPES[k]
        assert utils.coerce_type(typ_, typ_, v)


def test_coerce_type_to_text() -> None:
    assert utils.coerce_type("text", "bool", False) == str(False)
    # double into text
    assert utils.coerce_type("text", "double", -1726.1288) == "-1726.1288"
    # bytes to text (base64)
    assert utils.coerce_type("text", "binary", b'binary string') == "YmluYXJ5IHN0cmluZw=="
    # HexBytes to text (hex with prefix)
    assert utils.coerce_type("text", "binary", HexBytes(b'binary string')) == "0x62696e61727920737472696e67"


def test_coerce_type_to_bool() -> None:
    # text into bool
    assert utils.coerce_type("bool", "text", "False") is False
    assert utils.coerce_type("bool", "text", "yes") is True
    assert utils.coerce_type("bool", "text", "no") is False
    # some numeric types
    assert utils.coerce_type("bool", "bigint", 1) is True
    assert utils.coerce_type("bool", "bigint", 0) is False
    assert utils.coerce_type("bool", "decimal", Decimal(1)) is True
    assert utils.coerce_type("bool", "decimal", Decimal(0)) is False

    # no coercions
    with pytest.raises(ValueError):
        utils.coerce_type("bool", "complex", {"a": True})
    with pytest.raises(ValueError):
        utils.coerce_type("bool", "binary", b'True')
    with pytest.raises(ValueError):
        utils.coerce_type("bool", "timestamp", pendulum.now())


def test_coerce_type_to_double() -> None:
    # bigint into double
    assert utils.coerce_type("double", "bigint", 762162) == 762162.0
    # text into double if parsable
    assert utils.coerce_type("double", "text", " -1726.1288 ") == -1726.1288
    # hex text into double
    assert utils.coerce_type("double", "text",  "0xff") == 255.0
    # wei, decimal to double
    assert utils.coerce_type("double", "wei", Wei.from_int256(2137, decimals=2)) == 21.37
    assert utils.coerce_type("double", "decimal", Decimal("-1121.11")) == -1121.11
    # non parsable text
    with pytest.raises(ValueError):
        utils.coerce_type("double", "text", "a912.12")
    # bool does not coerce
    with pytest.raises(ValueError):
        utils.coerce_type("double", "bool", False)


def test_coerce_type_to_bigint() -> None:
    assert utils.coerce_type("bigint", "text", " -1726 ") == -1726
    # for round numerics we can convert
    assert utils.coerce_type("bigint", "double", -762162.0) == -762162
    assert utils.coerce_type("bigint", "decimal", Decimal("1276.0")) == 1276
    assert utils.coerce_type("bigint", "wei", Wei("1276.0")) == 1276

    # raises when not round
    with pytest.raises(ValueError):
        utils.coerce_type("bigint", "double", 762162.1)
    with pytest.raises(ValueError):
        utils.coerce_type("bigint", "decimal", Decimal(912.12))
    with pytest.raises(ValueError):
        utils.coerce_type("bigint", "wei", Wei(912.12))

    # non parsable floats and ints
    with pytest.raises(ValueError):
        utils.coerce_type("bigint", "text", "f912.12")
    with pytest.raises(ValueError):
        utils.coerce_type("bigint", "text", "912.12")


@pytest.mark.parametrize("dec_cls,data_type", [
    (Decimal, "decimal"),
    (Wei, "wei")
])
def test_coerce_to_numeric(dec_cls: Type[Any], data_type: TDataType) -> None:
    v = utils.coerce_type(data_type, "text", " -1726.839283 ")
    assert type(v) is dec_cls
    assert v == dec_cls("-1726.839283")
    v = utils.coerce_type(data_type, "bigint", -1726)
    assert type(v) is dec_cls
    assert v == dec_cls("-1726")
    # mind that 1276.37 does not have binary representation as used in float
    v = utils.coerce_type(data_type, "double", 1276.37)
    assert type(v) is dec_cls
    assert v.quantize(Decimal("1.00")) == dec_cls("1276.37")

    # wei to decimal and reverse
    v = utils.coerce_type(data_type, "decimal", Decimal("1276.37"))
    assert type(v) is dec_cls
    assert v == dec_cls("1276.37")
    v = utils.coerce_type(data_type, "wei", Wei("1276.37"))
    assert type(v) is dec_cls
    assert v == dec_cls("1276.37")

    # invalid format
    with pytest.raises(ValueError):
        utils.coerce_type(data_type, "text", "p912.12")


def test_coerce_type_from_hex_text() -> None:
    # hex text into various types
    assert utils.coerce_type("wei", "text", " 0xff") == 255
    assert utils.coerce_type("bigint", "text", " 0xff") == 255
    assert utils.coerce_type("decimal", "text", " 0xff") == Decimal(255)
    assert utils.coerce_type("double", "text", " 0xff") == 255.0


def test_coerce_type_to_timestamp() -> None:
    # timestamp cases
    assert utils.coerce_type("timestamp", "text", " 1580405246 ") == pendulum.parse("2020-01-30T17:27:26+00:00")
    # the tenths of microseconds will be ignored
    assert utils.coerce_type("timestamp", "double", 1633344898.7415245) == pendulum.parse("2021-10-04T10:54:58.741524+00:00")
    # if text is ISO string it will be coerced
    assert utils.coerce_type("timestamp", "text", "2022-05-10T03:41:31.466000+00:00") == pendulum.parse("2022-05-10T03:41:31.466000+00:00")
    assert utils.coerce_type("timestamp", "text", "2022-05-10T03:41:31.466+02:00") == pendulum.parse("2022-05-10T01:41:31.466Z")
    assert utils.coerce_type("timestamp", "text", "2022-05-10T03:41:31.466+0200") == pendulum.parse("2022-05-10T01:41:31.466Z")
    # parse almost ISO compliant string
    assert utils.coerce_type("timestamp", "text", "2022-04-26 10:36+02") == pendulum.parse("2022-04-26T10:36:00+02:00")
    assert utils.coerce_type("timestamp", "text", "2022-04-26 10:36") == pendulum.parse("2022-04-26T10:36:00+00:00")
    # parse date string
    assert utils.coerce_type("timestamp", "text", "2021-04-25") == pendulum.parse("2021-04-25", exact=True)

    # fails on "now" - yes pendulum by default parses "now" as .now()
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "text", "now")

    # fails on intervals - pendulum by default parses a string into: datetime, data, time or interval
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "text", "2007-03-01T13:00:00Z/2008-05-11T15:30:00Z")
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "text", "2011--20012")

    # test wrong unix timestamps
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "double", -1000000000000000000000000000)
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "double", 1000000000000000000000000000)

    # formats with timezones are not parsed
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "text", "06/04/22, 11:15PM IST")

    # we do not parse RFC 822, 2822, 850 etc.
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "text", "Wed, 06 Jul 2022 11:58:08 +0200")
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "text", "Tuesday, 13-Sep-22 18:42:31 UTC")

    # time data type not supported yet
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "text", "10:36")

    # non parsable datetime
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "text", "x2022-05-10T03:41:31.466000X+00:00")


def test_coerce_type_to_binary() -> None:
    # from hex string
    assert utils.coerce_type("binary", "text", "0x30") == b'0'
    # from base64
    assert utils.coerce_type("binary", "text", "YmluYXJ5IHN0cmluZw==") == b'binary string'
    # int into bytes
    assert utils.coerce_type("binary", "bigint", 15) == b"\x0f"
    # can't into double
    with pytest.raises(ValueError):
        utils.coerce_type("binary", "double", 912.12)
    # can't broken base64
    with pytest.raises(ValueError):
        assert utils.coerce_type("binary", "text", "!YmluYXJ5IHN0cmluZw==")


def test_py_type_to_sc_type() -> None:
    assert utils.py_type_to_sc_type(bool) == "bool"
    assert utils.py_type_to_sc_type(int) == "bigint"
    assert utils.py_type_to_sc_type(float) == "double"
    assert utils.py_type_to_sc_type(str) == "text"
    assert utils.py_type_to_sc_type(type(pendulum.now())) == "timestamp"
    assert utils.py_type_to_sc_type(type(datetime.datetime(1988, 12, 1))) == "timestamp"
    assert utils.py_type_to_sc_type(type(Decimal(1))) == "decimal"
    assert utils.py_type_to_sc_type(type(HexBytes("0xFF"))) == "binary"
    assert utils.py_type_to_sc_type(type(Wei.from_int256(2137, decimals=2))) == "wei"
    # unknown types raise TypeException
    with pytest.raises(TypeError):
        utils.py_type_to_sc_type(Any)
    # none type raises TypeException
    with pytest.raises(TypeError):
        utils.py_type_to_sc_type(type(None))
    # complex types
    assert utils.py_type_to_sc_type(list) == "complex"
    # assert utils.py_type_to_sc_type(set) == "complex"
    assert utils.py_type_to_sc_type(dict) == "complex"
    assert utils.py_type_to_sc_type(tuple) == "complex"
    assert utils.py_type_to_sc_type(Mapping) == "complex"
    assert utils.py_type_to_sc_type(MutableSequence) == "complex"


def test_coerce_type_complex() -> None:
    # dicts and lists should be coerced into strings automatically
    v_list = [1, 2, "3", {"complex": True}]
    v_dict = {"list": [1, 2], "str": "complex"}
    assert utils.py_type_to_sc_type(type(v_list)) == "complex"
    assert utils.py_type_to_sc_type(type(v_dict)) == "complex"
    assert type(utils.coerce_type("complex", "complex", v_dict)) is str
    assert type(utils.coerce_type("complex", "complex", v_list)) is str
    assert utils.coerce_type("complex", "complex", v_dict) == json.dumps(v_dict)
    assert utils.coerce_type("complex", "complex", v_list) == json.dumps(v_list)
    assert utils.coerce_type("text", "complex", v_dict) == json.dumps(v_dict)
    assert utils.coerce_type("text", "complex", v_list) == json.dumps(v_list)
    # all other coercions fail
    with pytest.raises(ValueError):
        utils.coerce_type("binary", "complex", v_list)
