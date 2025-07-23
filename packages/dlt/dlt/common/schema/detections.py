import datetime  # noqa: 251
from typing import Any, Optional, Type

from hexbytes import HexBytes

from dlt.common.pendulum import pendulum
from dlt.common.wei import Wei
from dlt.common.data_types import TDataType
from dlt.common.time import parse_iso_like_datetime


_NOW_TS: float = pendulum.now().timestamp()
_FLOAT_TS_RANGE = 5 * 31536000.0  # seconds in year


def is_timestamp(t: Type[Any], v: Any) -> Optional[TDataType]:
    # autodetect int and float withing 1 year range of NOW
    if t in [int, float]:
        if v >= _NOW_TS - _FLOAT_TS_RANGE and v <= _NOW_TS + _FLOAT_TS_RANGE:
            return "timestamp"
    return None


def is_iso_timestamp(t: Type[Any], v: Any) -> Optional[TDataType]:
    # only strings can be converted
    if not issubclass(t, str):
        return None
    if not v:
        return None
    # strict autodetection of iso timestamps
    try:
        dtv = parse_iso_like_datetime(v)
        if isinstance(dtv, datetime.datetime):
            return "timestamp"
    except Exception:
        pass
    return None


def is_iso_date(t: Type[Any], v: Any) -> Optional[TDataType]:
    # only strings can be converted
    if not issubclass(t, str):
        return None
    if not v:
        return None
    # don't cast iso timestamps as dates
    if is_iso_timestamp(t, v):
        return None
    # strict autodetection of iso timestamps
    try:
        dtv = parse_iso_like_datetime(v)
        if isinstance(dtv, datetime.date):
            return "date"
    except Exception:
        pass
    return None


def is_large_integer(t: Type[Any], v: Any) -> Optional[TDataType]:
    # only ints can be converted
    if issubclass(t, int):
        # TODO: this is bigquery limit, we need to implement better wei type
        # if integer does not find in maximum wei then convert to string
        if v > 578960446186580977117854925043439539266:
            return "text"
        # if integer does not fit into 64 bit unsigned int
        if v > 2**64 // 2 - 1:
            return "wei"

    return None


def is_hexbytes_to_text(t: Type[Any], v: Any) -> Optional[TDataType]:
    # HexBytes should be converted to text
    if issubclass(t, HexBytes):
        return "text"
    return None


def is_wei_to_double(t: Type[Any], v: Any) -> Optional[TDataType]:
    # Wei should be converted to double, use this only for aggregate non-financial reporting
    if issubclass(t, Wei):
        return "double"
    return None
