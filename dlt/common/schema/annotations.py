import inspect

from typing import Type, List, Any, Tuple, Union, Dict
from typing_extensions import Annotated, get_origin, get_args
from decimal import Decimal
from dlt.common.data_types import py_type_to_sc_type

from dataclasses import dataclass
from dlt.common.schema.typing import TTableSchema, TColumnSchema, TWriteDisposition, TDataType
from datetime import datetime, date


#
# base dataclasses used for hints
#
@dataclass
class BoolValue:
    value: bool


@dataclass
class StringValue:
    value: str


@dataclass
class IntValue:
    value: int


@dataclass
class StrListValue:
    value: List[str]


#
# Column Hints
#
class PrimaryKey(BoolValue): ...


class Unique(BoolValue): ...


#
# Table Hints
#
class TableName(StringValue): ...


class Description(StringValue): ...


class Classifiers(StringValue): ...


@dataclass
class WriteDisposition:
    value: TWriteDisposition


#
# Converters
#


def unwrap(t: Type[Any]) -> Tuple[Any, List[Any]]:
    """Returns python type info and wrapped types if this was annotated type"""
    if get_origin(t) is Annotated:
        args = get_args(t)
        return args[0], list(args[1:])
    return t, []


def to_full_type(t: Type[Any]) -> TColumnSchema:
    result: TColumnSchema = {}
    if get_origin(t) is Union:
        for arg in get_args(t):
            if arg is type(None):
                result["nullable"] = True
            else:
                result["data_type"] = py_type_to_sc_type(arg)
    else:
        result["data_type"] = py_type_to_sc_type(t)
    return result


def to_table_hints(h: List[Type[Any]]) -> TTableSchema:
    result: TTableSchema = {}
    for hint in h:
        if isinstance(hint, TableName):
            result["name"] = hint.value
        elif isinstance(hint, WriteDisposition):
            result["write_disposition"] = hint.value
    return result


def resolve_boolean_hint(column: TColumnSchema, hint: Type[Any], type: Type[Any], key: str) -> None:
    """boolean hints may be instantiated with a value or just be present as a class, in that case they are assumed to be true"""
    if isinstance(hint, type):
        column[key] = hint.value  # type: ignore
    if hint is type:
        column[key] = True  # type: ignore


def to_column_hints(h: List[Type[Any]]) -> TColumnSchema:
    result: TColumnSchema = {}
    for hint in h:
        resolve_boolean_hint(result, hint, Unique, "unique")
        resolve_boolean_hint(result, hint, PrimaryKey, "primary_key")
        #
        if isinstance(hint, Classifiers):
            result["x-classifiers"] = hint.value # NOTE: classifiers not supported properly yet
    return result


def class_to_table(ctt: Type[Any]) -> TTableSchema:

    cls, hints = unwrap(ctt)

    # initial checks
    if not inspect.isclass(cls):
        return {}

    table: TTableSchema = {"columns": {}}
    if hints:
        table = {**table, **to_table_hints(hints)}

    # return if there are no type annotations
    if not (annotations := getattr(cls, "__annotations__", None)):
        return table

    # convert annotations to table schema
    for name, value in annotations.items():
        t, hints = unwrap(value)

        # skip private attributes for now
        if name.startswith("_"):
            continue

        # extract column hints
        table["columns"][name] = {
            "name": name,
            **to_full_type(t),
            **to_column_hints(hints),
        }

    return table


def table_to_class(table: TTableSchema) -> str:
    """TODO: do something with ast unparse"""
    return ""
