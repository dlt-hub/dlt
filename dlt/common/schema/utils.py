import re
import base64
import binascii
import datetime
from dateutil.parser import isoparse
from typing import Type, Any, Mapping, Sequence

from dlt.common import pendulum, json, Decimal
from dlt.common.arithmetics import ConversionSyntax
from dlt.common.typing import StrStr
from dlt.common.schema.typing import StoredSchema, Table, ColumnBase, Column, ColumnProp, DataType, HintType, COLUMN_PROPS, COLUMN_HINTS, DATA_TYPES
from dlt.common.schema.exceptions import SchemaCorruptedException, SchemaEngineNoUpgradePathException


RE_LEADING_DIGITS = re.compile(r"^\d+")
RE_NON_ALPHANUMERIC = re.compile(r"[^a-zA-Z\d]")


# fix a name so it is acceptable as schema name
def normalize_schema_name(name: str) -> str:
    return RE_NON_ALPHANUMERIC.sub("", name).lower()


def verify_column(table_name: str, column_name: str, column: Column) -> None:
    existing_props = set(column.keys())
    missing_props = COLUMN_PROPS.difference(existing_props)
    if len(missing_props) > 0:
        raise SchemaCorruptedException(f"In table {table_name} column {column_name}: Column definition is missing following properties {missing_props}")
    data_type = column["data_type"]
    if data_type not in DATA_TYPES:
        raise SchemaCorruptedException(f"In table {table_name} column {column_name}: {data_type} is not one of available types: {DATA_TYPES}")
    for p, v in column.items():
        if p in COLUMN_HINTS and not isinstance(v, bool):
                raise SchemaCorruptedException(f"In table {table_name} column {column_name}: hint {p} is not boolean.")


def upgrade_engine_version(schema_dict: StoredSchema, from_engine: int, to_engine: int) -> None:
    if from_engine == 1:
        schema_dict["engine_version"] = 2
        schema_dict["includes"] = []
        schema_dict["excludes"] = []
        from_engine = 2
    if from_engine == 2:
        pass
    if from_engine != to_engine:
        raise SchemaEngineNoUpgradePathException(schema_dict["name"], schema_dict["engine_version"], from_engine, to_engine)



def add_missing_hints(column: ColumnBase) -> Column:
    return {
        **{  # type:ignore
            "partition": False,
            "cluster": False,
            "unique": False,
            "sort": False,
            "primary_key": False,
            "foreign_key": False,
        },
        **column
    }


def py_type_to_sc_type(t: Type[Any]) -> DataType:
    if t is float:
        return "double"
    elif t is int:
        return "bigint"
    elif t is bool:
        return "bool"
    elif issubclass(t, bytes):
        return "binary"
    elif issubclass(t, dict) or  issubclass(t, list):
        return "complex"
    elif issubclass(t, Decimal):
        return "decimal"
    elif issubclass(t, datetime.datetime):
        return "timestamp"
    else:
        return "text"


def coerce_type(to_type: DataType, from_type: DataType, value: Any) -> Any:
    if to_type == from_type:
        return value

    if to_type == "text":
        if from_type == "complex":
            return json.dumps(value)
        else:
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

    if to_type in ["wei", "bigint"]:
        if from_type == "bigint":
            return value
        if from_type in ["decimal", "double"]:
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

    if to_type == "decimal":
        if from_type in ["bigint", "wei"]:
            return value
        if from_type == "double":
            return Decimal(value)
        if from_type == "text":
            trim_value = value.strip()
            if trim_value.startswith("0x"):
                return int(trim_value[2:], 16)
            elif "." not in trim_value and "e" not in trim_value:
                return int(trim_value)
            else:
                try:
                    return Decimal(trim_value)
                except ConversionSyntax:
                    raise ValueError(trim_value)

    if to_type == "timestamp":
        if from_type in ["bigint", "double"]:
            # returns ISO datetime with timezone
            return str(pendulum.from_timestamp(value))

        if from_type == "text":
            # if parses as ISO date then pass it
            try:
                isoparse(value)
                return value
            except ValueError:
                # try to convert string to integer, or float
                try:
                    value = int(value)
                except ValueError:
                    # raises ValueError if not parsing correctly
                    value = float(value)
                return str(pendulum.from_timestamp(value))

    raise ValueError(value)


def compare_columns(a: Column, b: Column) -> bool:
    return a["data_type"] == b["data_type"] and a["nullable"] == b["nullable"]


def hint_to_column_prop(h: HintType) -> ColumnProp:
    if h == "not_null":
        return "nullable"
    return h


def version_table() -> Table:
    return {
        "version": add_missing_hints({
            "name": "version",
            "data_type": "bigint",
            "nullable": False,
        }),
        "engine_version": add_missing_hints({
            "name": "engine_version",
            "data_type": "bigint",
            "nullable": False
        }),
        "inserted_at": add_missing_hints({
            "name": "inserted_at",
            "data_type": "timestamp",
            "nullable": False
        })
    }

def load_table() -> Table:
    return {
        "load_id": add_missing_hints({
            "name": "load_id",
            "data_type": "text",
            "nullable": False
        }),
        "status": add_missing_hints({
            "name": "status",
            "data_type": "bigint",
            "nullable": False
        }),
        "inserted_at": add_missing_hints({
            "name": "inserted_at",
            "data_type": "timestamp",
            "nullable": False
        })
    }


def standard_hints() -> Mapping[HintType, Sequence[str]]:
    return {
        "not_null": ["^_record_hash$", "^_root_hash$", "^_parent_hash$", "^_pos$", "_load_id"],
        "foreign_key": ["^_parent_hash$"],
        "unique": ["^_record_hash$"]
    }
