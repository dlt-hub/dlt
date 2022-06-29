import datetime  # noqa: 251
from typing import Any, Optional, Type

from dlt.common import pendulum
from dlt.common.schema.typing import TDataType


_NOW_TS: float = pendulum.now().timestamp()
_FLOAT_TS_RANGE = 31536000.0  # seconds in year


def is_timestamp(t: Type[Any], v: Any) -> Optional[TDataType]:
    # autodetect int and float withing 1 year range of NOW
    if t in [int, float]:
        if v >= _NOW_TS - _FLOAT_TS_RANGE and v <= _NOW_TS + _FLOAT_TS_RANGE:
            return "timestamp"
    return None


def is_iso_timestamp(t: Type[Any], v: Any) -> Optional[TDataType]:
    # only strings can be converted
    if t is not str:
        return None
    if not v:
        return None
    # strict autodetection of iso timestamps
    try:
        dt = pendulum.parse(v, strict=True, exact=True)
        if isinstance(dt, datetime.datetime):
            return "timestamp"
    except Exception:
        pass
    return None

