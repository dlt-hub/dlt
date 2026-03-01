import binascii
import base64
import dataclasses
import datetime  # noqa: I251
from collections.abc import Mapping as C_Mapping, Sequence as C_Sequence
from functools import lru_cache
from typing import Any, Type, Union, Callable, Dict, Tuple
from enum import Enum

from dlt.common.json import custom_pua_remove, json
from dlt.common.json._simplejson import custom_encode as json_custom_encode
from dlt.common.typing import copy_sig
from dlt.common.wei import Wei
from dlt.common.arithmetics import InvalidOperation, Decimal
from dlt.common.data_types.typing import TDataType
from dlt.common.time import (
    ensure_pendulum_datetime_non_utc,
    ensure_pendulum_date,
    ensure_pendulum_time,
)
from dlt.common.utils import map_nested_values_in_place, str2bool
from dlt.common.pendulum import pendulum
from pendulum import (  # noqa: I251
    DateTime as PendulumDateTime,
    Date as PendulumDate,
    Time as PendulumTime,
)


# fast type map for common Python types to schema types.
PY_TYPE_TO_SC_TYPE: Dict[Type[Any], TDataType] = {
    str: "text",
    int: "bigint",
    float: "double",
    bool: "bool",
    dict: "json",
    list: "json",
    datetime.datetime: "timestamp",
    datetime.date: "date",
    datetime.time: "time",
    PendulumDateTime: "timestamp",
    PendulumDate: "date",
    PendulumTime: "time",
}


def _py_type_to_sc_type(t: Type[Any]) -> TDataType:
    if result := PY_TYPE_TO_SC_TYPE.get(t):
        return result

    # wei is subclass of decimal and must be checked first
    if issubclass(t, (dict, list)):
        return "json"
    if issubclass(t, Wei):
        return "wei"
    if issubclass(t, Decimal):
        return "decimal"
    # datetime is subclass of date and must be checked first
    if issubclass(t, datetime.datetime):
        return "timestamp"
    if issubclass(t, datetime.date):
        return "date"
    if issubclass(t, datetime.time):
        return "time"
    # subclassed basic types (bool cannot be subclassed)
    if issubclass(t, str):
        return "text"
    if issubclass(t, float):
        return "double"
    if issubclass(t, int):
        return "bigint"
    if issubclass(t, bytes):
        return "binary"
    if dataclasses.is_dataclass(t) or issubclass(t, (C_Mapping, C_Sequence)):
        return "json"
    # Enum is coerced to str or int respectively
    if issubclass(t, Enum):
        if issubclass(t, int):
            return "bigint"
        else:
            # str subclass and unspecified enum type translates to text
            return "text"

    raise TypeError(t)


# preserves original signature which lru_cache destroys
py_type_to_sc_type = copy_sig(_py_type_to_sc_type)(lru_cache(maxsize=None)(_py_type_to_sc_type))


def json_to_str(value: Any) -> str:
    return json.dumps(map_nested_values_in_place(custom_pua_remove, value))


def coerce_from_date_types(
    to_type: TDataType, value: datetime.datetime
) -> Union[datetime.datetime, datetime.date, datetime.time, int, float, str]:
    v = ensure_pendulum_datetime_non_utc(value)
    if to_type == "timestamp":
        return v
    if to_type == "text":
        return v.isoformat()
    if to_type == "bigint":
        return v.int_timestamp
    if to_type == "double":
        return v.timestamp()
    if to_type == "date":
        return ensure_pendulum_date(v)
    if to_type == "time":
        return v.time()
    raise TypeError(f"Cannot convert timestamp to `{to_type}`")


def _text_to_binary(value: str) -> bytes:
    if value.startswith("0x"):
        return bytes.fromhex(value[2:])
    try:
        return base64.b64decode(value, validate=True)
    except binascii.Error:
        raise ValueError(value)


def _text_to_bigint(value: str) -> int:
    trim_value = value.strip()
    if trim_value.startswith("0x"):
        return int(trim_value[2:], 16)
    return int(trim_value)


def _text_to_double(value: str) -> float:
    trim_value = value.strip()
    if trim_value.startswith("0x"):
        return float(int(trim_value[2:], 16))
    return float(trim_value)


def _text_to_decimal(value: str) -> Decimal:
    trim_value = value.strip()
    if trim_value.startswith("0x"):
        return Decimal(int(trim_value[2:], 16))
    try:
        return Decimal(trim_value)
    except InvalidOperation:
        raise ValueError(trim_value)


def _text_to_wei(value: str) -> Wei:
    trim_value = value.strip()
    if trim_value.startswith("0x"):
        return Wei(int(trim_value[2:], 16))
    try:
        return Wei(trim_value)
    except InvalidOperation:
        raise ValueError(trim_value)


def _numeric_to_bigint(value: Any) -> int:
    if value % 1 != 0:
        raise ValueError(value)
    return int(value)


def _text_fallback(value: Any) -> str:
    try:
        return str(json_custom_encode(value))
    except TypeError:
        return str(value)


def _text_to_date_fast(value: str) -> PendulumDate:
    """Fast date parsing: native Python + pendulum Date."""
    try:
        d = datetime.date.fromisoformat(value)
        return PendulumDate(d.year, d.month, d.day)
    except Exception:
        return ensure_pendulum_date(value)


_COERCE_DISPATCH: Dict[Tuple[TDataType, TDataType], Callable[[Any], Any]] = {
    # to text
    ("text", "json"): json_to_str,
    ("text", "bigint"): _text_fallback,
    ("text", "double"): _text_fallback,
    ("text", "wei"): _text_fallback,
    ("text", "decimal"): _text_fallback,
    ("text", "bool"): _text_fallback,
    ("text", "binary"): _text_fallback,
    ("text", "timestamp"): _text_fallback,
    ("text", "date"): _text_fallback,
    ("text", "time"): _text_fallback,
    # to binary
    ("binary", "text"): _text_to_binary,
    ("binary", "bigint"): lambda v: v.to_bytes((v.bit_length() + 7) // 8, "little"),
    # to bigint
    ("bigint", "text"): _text_to_bigint,
    ("bigint", "wei"): _numeric_to_bigint,
    ("bigint", "decimal"): _numeric_to_bigint,
    ("bigint", "double"): _numeric_to_bigint,
    # to double
    ("double", "text"): _text_to_double,
    ("double", "bigint"): float,
    ("double", "wei"): float,
    ("double", "decimal"): float,
    # to decimal
    ("decimal", "text"): _text_to_decimal,
    ("decimal", "bigint"): Decimal,
    ("decimal", "double"): Decimal,
    ("decimal", "wei"): Decimal,
    # to wei
    ("wei", "text"): _text_to_wei,
    ("wei", "bigint"): Wei,
    ("wei", "double"): Wei,
    ("wei", "decimal"): Wei,
    # to bool
    ("bool", "text"): str2bool,
    ("bool", "bigint"): bool,
    ("bool", "double"): bool,
    ("bool", "decimal"): bool,
    ("bool", "wei"): bool,
    # to json
    ("json", "text"): json.loads,
    # to timestamp
    ("timestamp", "text"): ensure_pendulum_datetime_non_utc,
    ("timestamp", "bigint"): pendulum.from_timestamp,
    ("timestamp", "double"): pendulum.from_timestamp,
    ("timestamp", "date"): ensure_pendulum_datetime_non_utc,
    # to date (fast path)
    ("date", "text"): _text_to_date_fast,
    ("date", "timestamp"): ensure_pendulum_date,
    ("date", "bigint"): ensure_pendulum_date,
    ("date", "double"): ensure_pendulum_date,
    # to time
    ("time", "text"): ensure_pendulum_time,
}


def coerce_value(to_type: TDataType, from_type: TDataType, value: Any) -> Any:
    if to_type == from_type:
        if to_type == "json":
            return map_nested_values_in_place(custom_pua_remove, value)
        if hasattr(value, "value"):
            if to_type == "text":
                return str(value.value)
            elif to_type == "bigint":
                return int(value.value)
        return value

    # dispatch table lookup
    coercer = _COERCE_DISPATCH.get((to_type, from_type))
    if coercer is not None:
        try:
            return coercer(value)
        except OverflowError as e:
            raise ValueError(value) from e

    raise ValueError(value)
