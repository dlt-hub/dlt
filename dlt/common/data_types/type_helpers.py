import binascii
import base64
import datetime  # noqa: I251
from collections.abc import Mapping as C_Mapping, Sequence as C_Sequence
from typing import Any, Type, Literal, Union, cast

from dlt.common import pendulum, json, Decimal, Wei
from dlt.common.json import custom_pua_remove
from dlt.common.json._simplejson import custom_encode as json_custom_encode
from dlt.common.arithmetics import InvalidOperation
from dlt.common.data_types.typing import TDataType
from dlt.common.time import ensure_pendulum_datetime, parse_iso_like_datetime, ensure_pendulum_date
from dlt.common.utils import map_nested_in_place, str2bool


def py_type_to_sc_type(t: Type[Any]) -> TDataType:
    # start with most popular types
    if t is str:
        return "text"
    if t is float:
        return "double"
    # bool is subclass of int so must go first
    if t is bool:
        return "bool"
    if t is int:
        return "bigint"
    if issubclass(t, (dict, list)):
        return "complex"

    # those are special types that will not be present in json loaded dict
    # wei is subclass of decimal and must be checked first
    if issubclass(t, Wei):
        return "wei"
    if issubclass(t, Decimal):
        return "decimal"
    # datetime is subclass of date and must be checked first
    if issubclass(t, datetime.datetime):
        return "timestamp"
    if issubclass(t, datetime.date):
        return "date"
    # check again for subclassed basic types
    if issubclass(t, str):
        return "text"
    if issubclass(t, float):
        return "double"
    if issubclass(t, int):
        return "bigint"
    if issubclass(t, bytes):
        return "binary"
    if issubclass(t, (C_Mapping, C_Sequence)):
        return "complex"

    raise TypeError(t)


def complex_to_str(value: Any) -> str:
    return json.dumps(map_nested_in_place(custom_pua_remove, value))


def coerce_to_date_types(
    to_type: Literal["timestamp", "date"], from_type: TDataType, value: Any
) -> Union[datetime.datetime, datetime.date]:
    result: Union[datetime.datetime, datetime.date]
    try:
        if from_type in ["bigint", "double"]:
            # returns ISO datetime with timezone
            result = pendulum.from_timestamp(value)

        elif from_type == "text":
            try:
                result = parse_iso_like_datetime(value)
            except ValueError:
                # try to convert string to integer, or float
                try:
                    value = float(value)
                    result = pendulum.from_timestamp(value)
                except ValueError:
                    raise ValueError(value)
        elif from_type in ["timestamp", "date"]:
            result = value
        else:
            raise ValueError(value)
    except OverflowError:
        # when parsed data is converted to integer and must stay within some size
        # id that is not possible OverflowError is raised and text cannot be represented as datetime
        raise ValueError(value)
    except Exception:
        # catch all problems in pendulum
        raise ValueError(value)
    # Pendulum ISO parsing may result in either datetime or date
    if to_type == "date":
        return ensure_pendulum_date(result)
    return ensure_pendulum_datetime(result)


def coerce_from_date_types(
    to_type: TDataType, value: datetime.datetime
) -> Union[datetime.datetime, datetime.date, int, float, str]:
    v = ensure_pendulum_datetime(value)
    if to_type == "timestamp":
        return v
    if to_type == "text":
        return v.isoformat()
    if to_type == "bigint":
        return v.int_timestamp  # type: ignore
    if to_type == "double":
        return v.timestamp()  # type: ignore
    if to_type == "date":
        return ensure_pendulum_date(v)
    raise TypeError(f"Cannot convert timestamp to {to_type}")


def coerce_value(to_type: TDataType, from_type: TDataType, value: Any) -> Any:
    if to_type == from_type:
        if to_type == "complex":
            # complex types need custom encoding to be removed
            return map_nested_in_place(custom_pua_remove, value)
        return value

    if to_type == "text":
        if from_type == "complex":
            return complex_to_str(value)
        else:
            # use the same string encoding as in json
            try:
                return json_custom_encode(value)
            except TypeError:
                # for other types use internal conversion
                return str(value)

    if to_type == "binary":
        if from_type == "text":
            if value.startswith("0x"):
                return bytes.fromhex(value[2:])
            try:
                return base64.b64decode(value, validate=True)
            except binascii.Error:
                raise ValueError(value)
        if from_type == "bigint":
            return value.to_bytes((value.bit_length() + 7) // 8, 'little')

    if to_type == "bigint":
        if from_type in ["wei", "decimal", "double"]:
            if value % 1 != 0:
                # only integer decimals and floats can be coerced
                raise ValueError(value)
            return int(value)
        if from_type == "text":
            trim_value = value.strip()
            if trim_value.startswith("0x"):
                return int(trim_value[2:], 16)
            else:
                return int(trim_value)

    if to_type == "double":
        if from_type in ["bigint", "wei", "decimal"]:
            return float(value)
        if from_type == "text":
            trim_value = value.strip()
            if trim_value.startswith("0x"):
                return float(int(trim_value[2:], 16))
            else:
                return float(trim_value)

    # decimal and wei behave identically when converted from/to
    if to_type in ["decimal", "wei"]:
        # get target class
        decimal_cls = Decimal if to_type == "decimal" else Wei

        if from_type in ["bigint", "wei", "decimal", "double"]:
            return decimal_cls(value)
        if from_type == "text":
            trim_value = value.strip()
            if trim_value.startswith("0x"):
                return decimal_cls(int(trim_value[2:], 16))
            # elif "." not in trim_value and "e" not in trim_value:
            #     return int(trim_value)
            else:
                try:
                    return decimal_cls(trim_value)
                except InvalidOperation:
                    raise ValueError(trim_value)

    if to_type in ["timestamp", "date"]:
        return coerce_to_date_types(cast(Literal["timestamp", "date"], to_type), from_type, value)

    if to_type == "bool":
        if from_type == "text":
            return str2bool(value)
        if from_type not in ["complex", "binary", "timestamp"]:
            # all the numeric types will convert to bool on 0 - False, 1 - True
            return bool(value)

    raise ValueError(value)
