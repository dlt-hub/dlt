from hexbytes import HexBytes

from dlt.common import pendulum, Decimal, Wei
from dlt.common.schema.utils import autodetect_sc_type
from dlt.common.schema.detections import is_hexbytes_to_text, is_timestamp, is_iso_timestamp, is_iso_date, is_large_integer, is_wei_to_double, _FLOAT_TS_RANGE, _NOW_TS


def test_timestamp_detection() -> None:
    # datetime.datetime
    assert is_timestamp(float, pendulum.now().timestamp()) == "timestamp"
    assert is_timestamp(int, pendulum.now().int_timestamp) == "timestamp"
    assert is_timestamp(str, pendulum.now().timestamp()) is None
    assert is_timestamp(float, _NOW_TS - _FLOAT_TS_RANGE - 0.1) is None
    assert is_timestamp(float, _NOW_TS + _FLOAT_TS_RANGE + 0.1) is None


def test_iso_timestamp_detection() -> None:
    assert is_iso_timestamp(str, str(pendulum.now())) == "timestamp"
    assert is_iso_timestamp(str, "1975-05-21T22:00:00Z") == "timestamp"
    assert is_iso_timestamp(str, "2022-06-01T00:48:35.040Z") == "timestamp"
    assert is_iso_timestamp(str, "1975-0521T22:00:00Z") == "timestamp"
    assert is_iso_timestamp(str, "2021-07-24 10:51") == "timestamp"
    # dates and times are not accepted
    assert is_iso_timestamp(str, "1975-05-21") is None
    assert is_iso_timestamp(str, "22:00:00") is None
    # wrong formats
    assert is_iso_timestamp(str, "0-05-01T27:00:00Z") is None
    assert is_iso_timestamp(str, "") is None
    assert is_iso_timestamp(str, "1975-05-01T27:00:00Z") is None
    assert is_iso_timestamp(str, "1975-0521T22 00:00") is None
    # culture specific RFCs will not be recognized
    assert is_iso_timestamp(str, "Wed, 29 Jun 2022 13:56:34 +0000") is None
    # wrong type
    assert is_iso_timestamp(float, str(pendulum.now())) is None


def test_iso_date_detection() -> None:
    assert is_iso_date(str, str(pendulum.now().date())) == "date"
    assert is_iso_date(str, "1975-05-21") == "date"

    # dont auto-detect timestamps as dates
    assert is_iso_date(str, str(pendulum.now())) is None
    assert is_iso_date(str, "1975-05-21T22:00:00Z") is None
    assert is_iso_date(str, "2022-06-01T00:48:35.040Z") is None
    assert is_iso_date(str, "1975-0521T22:00:00Z") is None
    assert is_iso_date(str, "2021-07-24 10:51") is None
    
    # times are not accepted
    assert is_iso_date(str, "22:00:00") is None
    # wrong formats
    assert is_iso_date(str, "0-05-01") is None
    assert is_iso_date(str, "") is None
    assert is_iso_date(str, "1975-05") is None
    assert is_iso_date(str, "1975") is None
    assert is_iso_date(str, "01-12") is None
    assert is_iso_date(str, "1975/05/01") is None

    # wrong type
    assert is_iso_date(float, str(pendulum.now().date())) is None


def test_detection_large_integer() -> None:
    assert is_large_integer(str, "A") is None
    assert is_large_integer(int, 2**64 // 2) == "wei"
    assert is_large_integer(int, 578960446186580977117854925043439539267) == "text"
    assert is_large_integer(int, 2**64 // 2 - 1) is None
    assert is_large_integer(int, -2**64 // 2 - 1) is None


def test_detection_hexbytes_to_text() -> None:
    assert is_hexbytes_to_text(bytes, b'hey') is None
    assert is_hexbytes_to_text(HexBytes, b'hey') == "text"


def test_wei_to_double() -> None:
    assert is_wei_to_double(Wei, Wei(1)) == "double"
    assert is_wei_to_double(Decimal, Decimal(1)) is None


def test_detection_function() -> None:
    assert autodetect_sc_type(None, str, str(pendulum.now())) is None
    assert autodetect_sc_type(["iso_timestamp"], str, str(pendulum.now())) == "timestamp"
    assert autodetect_sc_type(["iso_timestamp"], float, str(pendulum.now())) is None
    assert autodetect_sc_type(["iso_date"], str, str(pendulum.now().date())) == "date"
    assert autodetect_sc_type(["iso_date"], float, str(pendulum.now().date())) is None
    assert autodetect_sc_type(["timestamp"], str, str(pendulum.now())) is None
    assert autodetect_sc_type(["timestamp", "iso_timestamp"], float, pendulum.now().timestamp()) == "timestamp"
    assert autodetect_sc_type(["timestamp", "large_integer"], int, 2**64) == "wei"
    assert autodetect_sc_type(["large_integer", "hexbytes_to_text"], HexBytes, b'hey') == "text"
    assert autodetect_sc_type(["large_integer", "wei_to_double"], Wei, Wei(10**18)) == "double"
