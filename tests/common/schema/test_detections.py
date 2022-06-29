from dlt.common import pendulum
from dlt.common.schema.utils import autodetect_sc_type
from dlt.common.schema.detections import is_timestamp, is_iso_timestamp, _FLOAT_TS_RANGE, _NOW_TS


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
    assert is_iso_timestamp(str, "1975-0521T22:00:00Z") == "timestamp"
    # dates and times are not accepted
    assert is_iso_timestamp(str, "1975-05-21") is None
    assert is_iso_timestamp(str, "22:00:00") is None
    # wrong formats
    assert is_iso_timestamp(str, "0-05-01T27:00:00Z") is None
    assert is_iso_timestamp(str, "") is None
    assert is_iso_timestamp(str, "1975-05-01T27:00:00Z") is None
    assert is_iso_timestamp(str, "1975-0521T22 00:00") is None
    assert is_iso_timestamp(str, "Wed, 29 Jun 2022 13:56:34 +0000") is None
    # wrong type
    assert is_iso_timestamp(float, str(pendulum.now())) is None


def test_detection_function() -> None:
    assert autodetect_sc_type(None, str, str(pendulum.now())) is None
    assert autodetect_sc_type(["iso_timestamp"], str, str(pendulum.now())) == "timestamp"
    assert autodetect_sc_type(["iso_timestamp"], float, str(pendulum.now())) is None
    assert autodetect_sc_type(["timestamp"], str, str(pendulum.now())) is None
    assert autodetect_sc_type(["timestamp", "iso_timestamp"], float, pendulum.now().timestamp()) == "timestamp"
