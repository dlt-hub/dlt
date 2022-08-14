import pytest
import datetime  # noqa: I251
from hexbytes import HexBytes

from dlt.common import Decimal, json, pendulum
from dlt.common.schema import utils


def test_coerce_type_others() -> None:
    # same type coercion
    assert utils.coerce_type("double", "double", 8721.1) == 8721.1
    # anything into text
    assert utils.coerce_type("text", "bool", False) == str(False)
    # text into bool
    assert utils.coerce_type("bool", "text", "False") is False


def test_coerce_type_bool() -> None:
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
        utils.coerce_type("bool", "datetime", pendulum.now())


def test_coerce_type_double() -> None:
    # bigint into double
    assert utils.coerce_type("double", "bigint", 762162) == 762162.0
    # text into double if parsable
    assert utils.coerce_type("double", "text", " -1726.1288 ") == -1726.1288
    # hex text into double
    assert utils.coerce_type("double", "text", "0xff") == 255.0
    # double into text
    assert utils.coerce_type("text", "double", -1726.1288) == "-1726.1288"
    # double into bigint when not round
    with pytest.raises(ValueError):
        utils.coerce_type("bigint", "double", 762162.1)
    # works when round
    assert utils.coerce_type("bigint", "double", -762162.0) == -762162


def test_coerce_type_integer() -> None:
    # bigint/wei type
    assert utils.coerce_type("bigint", "text", " -1726 ") == -1726
    assert utils.coerce_type("wei", "text", " -1726 ") == -1726
    assert utils.coerce_type("bigint", "double", 1276.0) == 1276
    assert utils.coerce_type("wei", "double", 1276.0) == 1276
    assert utils.coerce_type("wei", "decimal", Decimal(1276.0)) == 1276
    assert utils.coerce_type("bigint", "decimal", 1276.0) == 1276
    # double into bigint raises
    with pytest.raises(ValueError):
        utils.coerce_type("bigint", "double", 912.12)
    with pytest.raises(ValueError):
        utils.coerce_type("wei", "double", 912.12)
    # decimal (non integer) also raises
    with pytest.raises(ValueError):
        utils.coerce_type("bigint", "decimal", Decimal(912.12))
    with pytest.raises(ValueError):
        utils.coerce_type("wei", "decimal", Decimal(912.12))
    # non parsable floats and ints
    with pytest.raises(ValueError):
        utils.coerce_type("bigint", "text", "f912.12")
    with pytest.raises(ValueError):
        utils.coerce_type("double", "text", "a912.12")


def test_coerce_type_decimal() -> None:
    # decimal type
    assert utils.coerce_type("decimal", "text", " -1726 ") == Decimal("-1726")
    # we keep integer if value is integer
    assert utils.coerce_type("decimal", "bigint", -1726) == -1726
    assert utils.coerce_type("decimal", "double", 1276.0) == Decimal("1276")


def test_coerce_type_from_hex_text() -> None:
    # hex text into various types
    assert utils.coerce_type("wei", "text", " 0xff") == 255
    assert utils.coerce_type("bigint", "text", " 0xff") == 255
    assert utils.coerce_type("decimal", "text", " 0xff") == Decimal(255)
    assert utils.coerce_type("double", "text", " 0xff") == 255.0


def test_coerce_type_timestamp() -> None:
    # timestamp cases
    assert utils.coerce_type("timestamp", "text", " 1580405246 ") == pendulum.parse("2020-01-30T17:27:26+00:00")
    # the tenths of microseconds will be ignored
    assert utils.coerce_type("timestamp", "double", 1633344898.7415245) == pendulum.parse("2021-10-04T10:54:58.741524+00:00")
    # if text is ISO string it will be coerced
    assert utils.coerce_type("timestamp", "text", "2022-05-10T03:41:31.466000+00:00") == pendulum.parse("2022-05-10T03:41:31.466000+00:00")
    # parse almost ISO compliant string
    assert utils.coerce_type("timestamp", "text", "2022-04-26 10:36") == pendulum.parse("2022-04-26T10:36:00+00:00")
    # parse date string
    assert utils.coerce_type("timestamp", "text", "2021-04-25") == pendulum.parse("2021-04-25", exact=True)
    # parse RFC 822 string
    assert utils.coerce_type("timestamp", "text", "Wed, 06 Jul 2022 11:58:08 +0000") == pendulum.parse("2022-07-06T11:58:08Z")
    # parse RFC RFC 2822
    # the case above interprets the year as 2006
    # assert utils.coerce_type("timestamp", "text", "Wednesday, 06-Jul-22 11:58:08 UTC") == pendulum.parse("2022-07-06T11:58:08Z")

    # time data type not supported yet
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "text", "10:36")

    # non parsable datetime
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "text", "x2022-05-10T03:41:31.466000X+00:00")


def test_coerce_type_binary() -> None:
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
    # bytes to text (base64)
    assert utils.coerce_type("text", "binary", b'binary string') == "YmluYXJ5IHN0cmluZw=="
    # HexBytes to text (hex with prefix)
    assert utils.coerce_type("text", "binary", HexBytes(b'binary string')) == "0x62696e61727920737472696e67"


def test_py_type_to_sc_type() -> None:
    assert utils.py_type_to_sc_type(type(pendulum.now())) == "timestamp"
    assert utils.py_type_to_sc_type(type(datetime.datetime(1988, 12, 1))) == "timestamp"
    assert utils.py_type_to_sc_type(type(Decimal(1))) == "decimal"
    assert utils.py_type_to_sc_type(type(HexBytes("0xFF"))) == "binary"


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
