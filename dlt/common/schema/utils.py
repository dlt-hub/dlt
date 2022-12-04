from importlib import import_module
import re
import base64
import binascii
import hashlib
import datetime  # noqa: I251
import contextlib
from copy import deepcopy
from collections.abc import Mapping as C_Mapping, Sequence as C_Sequence
from types import ModuleType
from typing import Dict, List, Sequence, Tuple, Type, Any, cast

from dlt.common import pendulum, json, Decimal, Wei
from pendulum.parsing import parse_iso8601, _parse_common as parse_datetime_common
from pendulum.tz import UTC
from dlt.common.json import custom_encode as json_custom_encode, custom_pua_remove
from dlt.common.arithmetics import InvalidOperation
from dlt.common.exceptions import DictValidationException
from dlt.common.normalizers.names import TNormalizeNameFunc
from dlt.common.typing import DictStrAny, REPattern
from dlt.common.utils import map_nested_in_place, str2bool
from dlt.common.validation import TCustomValidator, validate_dict
from dlt.common.schema import detections
from dlt.common.schema.typing import LOADS_TABLE_NAME, SIMPLE_REGEX_PREFIX, VERSION_TABLE_NAME, TColumnName, TNormalizersConfig, TPartialTableSchema, TSimpleRegex, TStoredSchema, TTableSchema, TTableSchemaColumns, TColumnSchemaBase, TColumnSchema, TColumnProp, TDataType, TColumnHint, TTypeDetectionFunc, TTypeDetections, TWriteDisposition
from dlt.common.schema.exceptions import CannotCoerceColumnException, ParentTableNotFoundException, SchemaEngineNoUpgradePathException, SchemaException, TablePropertiesConflictException


RE_LEADING_DIGITS = re.compile(r"^\d+")
RE_NON_ALPHANUMERIC = re.compile(r"[^a-zA-Z\d]")
RE_NON_ALPHANUMERIC_UNDERSCORE = re.compile(r"[^a-zA-Z\d_]")
DEFAULT_WRITE_DISPOSITION: TWriteDisposition = "append"


def apply_defaults(stored_schema: TStoredSchema) -> None:
    for table_name, table in stored_schema["tables"].items():
        # overwrite name
        table["name"] = table_name
        # add default write disposition to root tables
        if table.get("parent") is None and table.get("write_disposition") is None:
            table["write_disposition"] = DEFAULT_WRITE_DISPOSITION
        # add missing hints to columns
        for column_name in table["columns"]:
            # add default hints to tables
            column = add_missing_hints(table["columns"][column_name])
            # overwrite column name
            column["name"] = column_name
            # set column with default
            table["columns"][column_name] = column


def remove_defaults(stored_schema: TStoredSchema) -> None:
    clean_tables = deepcopy(stored_schema["tables"])
    for t in clean_tables.values():
        del t["name"]
        for c in t["columns"].values():
            # do not save names
            del c["name"]  # type: ignore
            # remove hints with default values
            for h in list(c.keys()):
                if isinstance(c[h], bool) and c[h] is False and h != "nullable":  # type: ignore
                    del c[h]  # type: ignore

    stored_schema["tables"] = clean_tables


def bump_version_if_modified(stored_schema: TStoredSchema) -> Tuple[int, str]:
    # if any change to schema document is detected then bump version and write new hash
    hash_ = generate_version_hash(stored_schema)
    previous_hash = stored_schema.get("version_hash")
    if not previous_hash:
        # if hash was not set, set it without bumping the version, that's initial schema
        pass
    elif hash_ != previous_hash:
        stored_schema["version"] += 1
    stored_schema["version_hash"] = hash_
    return stored_schema["version"], hash_

def generate_version_hash(stored_schema: TStoredSchema) -> str:
    # generates hash out of stored schema content, excluding the hash itself and version
    schema_copy = deepcopy(stored_schema)
    schema_copy.pop("version")
    schema_copy.pop("version_hash", None)
    schema_copy.pop("imported_version_hash", None)
    # ignore order of elements when computing the hash
    content = json.dumps(schema_copy, sort_keys=True)
    h = hashlib.sha3_256(content.encode("utf-8"))
    # additionally check column order
    table_names = sorted((schema_copy.get("tables") or {}).keys())
    if table_names:
        for tn in table_names:
            t = schema_copy["tables"][tn]
            h.update(tn.encode("utf-8"))
            # add column names to hash in order
            for cn in (t.get("columns") or {}).keys():
                h.update(cn.encode("utf-8"))
    return base64.b64encode(h.digest()).decode('ascii')


def verify_schema_hash(stored_schema: DictStrAny, empty_hash_verifies: bool = True) -> bool:
    # generates content hash and compares with existing
    current_hash: str = stored_schema.get("version_hash")
    if not current_hash and empty_hash_verifies:
        return True
    # if hash is present we can assume at least v4 engine version so hash is computable
    hash_ = generate_version_hash(cast(TStoredSchema, stored_schema))
    return hash_ == current_hash


def simple_regex_validator(path: str, pk: str, pv: Any, t: Any) -> bool:
    # custom validator on type TSimpleRegex
    if t is TSimpleRegex:
        if not isinstance(pv, str):
            raise DictValidationException(f"In {path}: field {pk} value {pv} has invalid type {type(pv).__name__} while str is expected", path, pk, pv)
        if pv.startswith(SIMPLE_REGEX_PREFIX):
            # check if regex
            try:
                re.compile(pv[3:])
            except Exception as e:
                raise DictValidationException(f"In {path}: field {pk} value {pv[3:]} does not compile as regex: {str(e)}", path, pk, pv)
        else:
            if RE_NON_ALPHANUMERIC_UNDERSCORE.match(pv):
                raise DictValidationException(f"In {path}: field {pk} value {pv} looks like a regex, please prefix with re:", path, pk, pv)
        # we know how to validate that type
        return True
    else:
        # don't know how to validate t
        return False


def column_name_validator(normalize_func: TNormalizeNameFunc) -> TCustomValidator:

    def validator(path: str, pk: str, pv: Any, t: Any) -> bool:
        if t is TColumnName:
            if not isinstance(pv, str):
                raise DictValidationException(f"In {path}: field {pk} value {pv} has invalid type {type(pv).__name__} while str is expected", path, pk, pv)
            if normalize_func(pv) != pv:
                raise DictValidationException(f"In {path}: field {pk}: {pv} is not a valid column name", path, pk, pv)
            return True
        else:
            return False

    return validator


def compile_simple_regex(r: TSimpleRegex) -> REPattern:
    if r.startswith(SIMPLE_REGEX_PREFIX):
        return re.compile(r[3:])
    else:
        # exact matches
        return re.compile("^" + re.escape(r) + "$")


def validate_stored_schema(stored_schema: TStoredSchema) -> None:
    # use lambda to verify only non extra fields
    validate_dict(TStoredSchema, stored_schema, ".", lambda k: not k.startswith("x-"), simple_regex_validator)
    # check child parent relationships
    for table_name, table in stored_schema["tables"].items():
        parent_table_name = table.get("parent")
        if parent_table_name:
            if parent_table_name not in stored_schema["tables"]:
                raise ParentTableNotFoundException(table_name, parent_table_name)


def migrate_schema(schema_dict: DictStrAny, from_engine: int, to_engine: int) -> TStoredSchema:
    if from_engine == to_engine:
        return cast(TStoredSchema, schema_dict)

    if from_engine == 1 and to_engine > 1:
        schema_dict["includes"] = []
        schema_dict["excludes"] = []
        from_engine = 2
    if from_engine == 2 and to_engine > 2:
        # current version of the schema
        current = cast(TStoredSchema, schema_dict)
        # add default normalizers and root hash propagation
        current["normalizers"] = default_normalizers()
        current["normalizers"]["json"]["config"] = {
                    "propagation": {
                        "root": {
                            "_dlt_id": "_dlt_root_id"
                        }
                    }
                }
        # move settings, convert strings to simple regexes
        d_h: Dict[TColumnHint, List[TSimpleRegex]] = schema_dict.pop("hints", {})
        for h_k, h_l in d_h.items():
            d_h[h_k] = list(map(lambda r: TSimpleRegex("re:" + r), h_l))
        p_t: Dict[TSimpleRegex, TDataType] = schema_dict.pop("preferred_types", {})
        p_t = {TSimpleRegex("re:" + k): v for k, v in p_t.items()}

        current["settings"] = {
            "default_hints": d_h,
            "preferred_types": p_t,
        }
        # repackage tables
        old_tables: Dict[str, TTableSchemaColumns] = schema_dict.pop("tables")
        current["tables"] = {}
        for name, columns in old_tables.items():
            # find last path separator
            parent = name
            # go back in a loop to find existing parent
            while True:
                idx = parent.rfind("__")
                if idx > 0:
                    parent = parent[:idx]
                    if parent not in old_tables:
                        continue
                else:
                    parent = None
                break
            nt = new_table(name, parent)
            nt["columns"] = columns
            current["tables"][name] = nt
        # assign exclude and include to tables

        def migrate_filters(group: str, filters: List[str]) -> None:
            # existing filter were always defined at the root table. find this table and move filters
            for f in filters:
                # skip initial ^
                root = f[1:f.find("__")]
                path = f[f.find("__") + 2:]
                t = current["tables"].get(root)
                if t is None:
                    # must add new table to hold filters
                    t = new_table(root)
                    current["tables"][root] = t
                t.setdefault("filters", {}).setdefault(group, []).append("re:^" + path)  # type: ignore

        excludes = schema_dict.pop("excludes", [])
        migrate_filters("excludes", excludes)
        includes = schema_dict.pop("includes", [])
        migrate_filters("includes", includes)

        # upgraded
        from_engine = 3
    if from_engine == 3 and to_engine > 3:
        # set empty version hash to pass validation, in engine 4 this hash is mandatory
        schema_dict.setdefault("version_hash", "")
        from_engine = 4
    if from_engine == 4 and to_engine > 4:
        # replace schema versions table
        schema_dict["tables"][VERSION_TABLE_NAME] = version_table()
        schema_dict["tables"][LOADS_TABLE_NAME] = load_table()
        from_engine = 5

    schema_dict["engine_version"] = from_engine
    if from_engine != to_engine:
        raise SchemaEngineNoUpgradePathException(schema_dict["name"], schema_dict["engine_version"], from_engine, to_engine)

    return cast(TStoredSchema, schema_dict)


def add_missing_hints(column: TColumnSchemaBase) -> TColumnSchema:
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


def autodetect_sc_type(detection_fs: Sequence[TTypeDetections], t: Type[Any], v: Any) -> TDataType:
    if detection_fs:
        for detection_fn in detection_fs:
            # the method must exist in the module
            detection_f: TTypeDetectionFunc = getattr(detections, "is_" + detection_fn)
            dt = detection_f(t, v)
            if dt is not None:
                return dt
    return None


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
    # TODO: implement new "date" type, currently assign "datetime"
    if issubclass(t, (datetime.datetime, datetime.date)):
        return "timestamp"

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


def coerce_value(to_type: TDataType, from_type: TDataType, value: Any) -> Any:
    if to_type == from_type:
        if to_type == "complex":
            # complex types will be always represented as strings
            return complex_to_str(value)
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

    if to_type == "timestamp":
        try:
            if from_type in ["bigint", "double"]:
                # returns ISO datetime with timezone
                return pendulum.from_timestamp(value)

            if from_type == "text":
                try:
                    # we use internal pendulum parse function. the generic function for example parses string "now" as now()
                    # it also tries to parse ISO intervals but the code is very low quality
                    # so it does not seem to be under control

                    # only iso dates are allowed
                    dtv = None
                    with contextlib.suppress(ValueError):
                        dtv = parse_iso8601(value)
                    # now try to parse a set of ISO like dates
                    if not dtv:
                        dtv = parse_datetime_common(value)
                    if isinstance(dtv, datetime.time):
                        raise ValueError(value)
                    if isinstance(dtv, datetime.datetime):
                        return pendulum.datetime(
                            dtv.year,
                            dtv.month,
                            dtv.day,
                            dtv.hour,
                            dtv.minute,
                            dtv.second,
                            dtv.microsecond,
                            tz=dtv.tzinfo or UTC  # type: ignore
                        )
                    return dtv
                except ValueError:
                    # try to convert string to integer, or float
                    try:
                        value = float(value)
                        return pendulum.from_timestamp(value)
                    except ValueError:
                        raise ValueError(value)
        except OverflowError:
            # when parsed data is converted to integer and must stay within some size
            # id that is not possible OverflowError is raised and text cannot be represented as datetime
            raise ValueError(value)
        except Exception:
            # catch all problems in pendulum
            raise ValueError(value)

    if to_type == "bool":
        if from_type == "text":
            return str2bool(value)
        if from_type not in ["complex", "binary", "timestamp"]:
            # all the numeric types will convert to bool on 0 - False, 1 - True
            return bool(value)

    raise ValueError(value)


def diff_tables(tab_a: TTableSchema, tab_b: TTableSchema, ignore_table_name: bool = True) -> TPartialTableSchema:
    table_name = tab_a["name"]
    if not ignore_table_name and table_name != tab_b["name"]:
        raise TablePropertiesConflictException(table_name, "name", table_name, tab_b["name"])

    # check if table properties can be merged
    if tab_a.get("parent") != tab_b.get("parent"):
        raise TablePropertiesConflictException(table_name, "parent", tab_a.get("parent"), tab_b.get("parent"))
    # check if partial table has write disposition set
    partial_w_d = tab_b.get("write_disposition")
    if partial_w_d:
        existing_w_d = tab_a.get("write_disposition")
        if existing_w_d != partial_w_d:
            raise TablePropertiesConflictException(table_name, "write_disposition", existing_w_d, partial_w_d)

    # get new columns, changes in the column data type or other properties are not allowed
    table_columns = tab_a["columns"]
    new_columns: List[TColumnSchema] = []
    for column in tab_b["columns"].values():
        column_name = column["name"]
        if column_name in table_columns:
            # we do not support changing existing columns
            if not compare_column(table_columns[column_name], column):
                # attempt to update to incompatible columns
                raise CannotCoerceColumnException(table_name, column_name, column["data_type"], table_columns[column_name]["data_type"], None)
        else:
            new_columns.append(column)

    # TODO: compare filters, description etc.

    # return partial table containing only name and properties that differ (column, filters etc.)
    return new_table(table_name, columns=new_columns)


def compare_tables(tab_a: TTableSchema, tab_b: TTableSchema) -> bool:
    try:
        diff_table = diff_tables(tab_a, tab_b, ignore_table_name=False)
        # columns cannot differ
        return len(diff_table["columns"]) == 0
    except SchemaException:
        return False


def merge_tables(table: TTableSchema, partial_table: TPartialTableSchema) -> TTableSchema:
    # merges "partial_table" into "table", preserving the "table" name
    diff_table = diff_tables(table, partial_table, ignore_table_name=True)
    # add new columns when all checks passed
    table["columns"].update(diff_table["columns"])
    return table


def compare_column(a: TColumnSchema, b: TColumnSchema) -> bool:
    return a["data_type"] == b["data_type"] and a["nullable"] == b["nullable"]


def hint_to_column_prop(h: TColumnHint) -> TColumnProp:
    if h == "not_null":
        return "nullable"
    return h


def version_table() -> TTableSchema:
    table = new_table(VERSION_TABLE_NAME, columns=[
            add_missing_hints({
                "name": "version",
                "data_type": "bigint",
                "nullable": False,
            }),
            add_missing_hints({
                "name": "engine_version",
                "data_type": "bigint",
                "nullable": False
            }),
            add_missing_hints({
                "name": "inserted_at",
                "data_type": "timestamp",
                "nullable": False
            }),
            add_missing_hints({
                "name": "schema_name",
                "data_type": "text",
                "nullable": False
            }),
            add_missing_hints({
                "name": "version_hash",
                "data_type": "text",
                "nullable": False
            }),
            add_missing_hints({
                "name": "schema",
                "data_type": "text",
                "nullable": False
            })
        ]
    )
    table["write_disposition"] = "skip"
    table["description"] = "Created by DLT. Tracks schema updates"
    return table


def load_table() -> TTableSchema:
    table = new_table(LOADS_TABLE_NAME, columns=[
            add_missing_hints({
                "name": "load_id",
                "data_type": "text",
                "nullable": False
            }),
            add_missing_hints({
                "name": "schema_name",
                "data_type": "text",
                "nullable": True
            }),
            add_missing_hints({
                "name": "status",
                "data_type": "bigint",
                "nullable": False
            }),
            add_missing_hints({
                "name": "inserted_at",
                "data_type": "timestamp",
                "nullable": False
            })
        ]
    )
    table["write_disposition"] = "skip"
    table["description"] = "Created by DLT. Tracks completed loads"
    return table


def new_table(
    table_name: str,
    parent_table_name: str = None,
    write_disposition: TWriteDisposition = None,
    columns: Sequence[TColumnSchema] = None,
    validate_schema: bool = False
) -> TTableSchema:

    table: TTableSchema = {
        "name": table_name,
        "columns": {} if columns is None else {c["name"]: add_missing_hints(c) for c in columns}
    }
    if parent_table_name:
        table["parent"] = parent_table_name
        assert write_disposition is None
    else:
        # set write disposition only for root tables
        table["write_disposition"] = write_disposition or DEFAULT_WRITE_DISPOSITION
    if validate_schema:
        validate_dict(TTableSchema, table, f"new_table/{table_name}")
    return table


def new_column(column_name: str, data_type: TDataType, nullable: bool = True, validate_schema: bool = False) -> TColumnSchema:
    column = add_missing_hints({
                "name": column_name,
                "data_type": data_type,
                "nullable": nullable
            })
    if validate_schema:
        validate_dict(TColumnSchema, column, f"new_column/{column_name}")
    return column


def default_normalizers() -> TNormalizersConfig:
    return {
                "detections": ["timestamp", "iso_timestamp"],
                "names": "dlt.common.normalizers.names.snake_case",
                "json": {
                    "module": "dlt.common.normalizers.json.relational"
                }
            }


def import_normalizers(normalizers_config: TNormalizersConfig) -> Tuple[ModuleType, ModuleType]:
    # TODO: type the modules with protocols
    naming_module = import_module(normalizers_config["names"])
    json_module = import_module(normalizers_config["json"]["module"])
    return naming_module, json_module


def standard_hints() -> Dict[TColumnHint, List[TSimpleRegex]]:
    return None
