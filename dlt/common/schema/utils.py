import re
import base64
import hashlib

from copy import deepcopy
from typing import Dict, List, Sequence, Tuple, Type, Any, cast, Union, Iterable, Optional

from dlt.common import json
from dlt.common.data_types import TDataType
from dlt.common.exceptions import DictValidationException
from dlt.common.normalizers import default_normalizers
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.typing import DictStrAny, REPattern
from dlt.common.validation import TCustomValidator, validate_dict
from dlt.common.schema import detections
from dlt.common.schema.typing import (SCHEMA_ENGINE_VERSION, LOADS_TABLE_NAME, SIMPLE_REGEX_PREFIX, VERSION_TABLE_NAME, TColumnName, TPartialTableSchema, TSchemaTables, TSchemaUpdate,
                                      TSimpleRegex, TStoredSchema, TTableSchema, TTableSchemaColumns, TColumnSchemaBase, TColumnSchema, TColumnProp,
                                      TColumnHint, TTypeDetectionFunc, TTypeDetections, TWriteDisposition)
from dlt.common.schema.exceptions import CannotCoerceColumnException, ParentTableNotFoundException, SchemaEngineNoUpgradePathException, SchemaException, TablePropertiesConflictException

# RE_LEADING_DIGITS = re.compile(r"^\d+")
# RE_NON_ALPHANUMERIC = re.compile(r"[^a-zA-Z\d]")
RE_NON_ALPHANUMERIC_UNDERSCORE = re.compile(r"[^a-zA-Z\d_]")
DEFAULT_WRITE_DISPOSITION: TWriteDisposition = "append"


def apply_defaults(stored_schema: TStoredSchema) -> None:
    for table_name, table in stored_schema["tables"].items():
        # overwrite name
        table["name"] = table_name
        # add default write disposition to root tables
        if table.get("parent") is None:
            if table.get("write_disposition") is None:
                table["write_disposition"] = DEFAULT_WRITE_DISPOSITION
            if table.get('resource') is None:
                table['resource'] = table_name
        # add missing hints to columns
        for column_name in table["columns"]:
            # add default hints to tables
            column = add_missing_hints(table["columns"][column_name])
            # overwrite column name
            column["name"] = column_name
            # set column with default
            table["columns"][column_name] = column


def remove_defaults(stored_schema: TStoredSchema) -> TStoredSchema:
    clean_tables = deepcopy(stored_schema["tables"])
    for table_name, t in clean_tables.items():
        del t["name"]
        if t.get('resource') == table_name:
            del t['resource']
        for c in t["columns"].values():
            # do not save names
            del c["name"]
            # remove hints with default values
            for h in list(c.keys()):
                if isinstance(c[h], bool) and c[h] is False and h != "nullable":  # type: ignore
                    del c[h]  # type: ignore

    stored_schema["tables"] = clean_tables
    return stored_schema


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


def verify_schema_hash(loaded_schema_dict: DictStrAny, verifies_if_not_migrated: bool = False) -> bool:
    # generates content hash and compares with existing
    engine_version: str = loaded_schema_dict.get("engine_version")
    # if upgrade is needed, the hash cannot be compared
    if verifies_if_not_migrated and engine_version != SCHEMA_ENGINE_VERSION:
        return True
    # if hash is present we can assume at least v4 engine version so hash is computable
    stored_schema = cast(TStoredSchema, loaded_schema_dict)
    hash_ = generate_version_hash(stored_schema)
    return hash_ == stored_schema["version_hash"]


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


def column_name_validator(naming: NamingConvention) -> TCustomValidator:

    def validator(path: str, pk: str, pv: Any, t: Any) -> bool:
        if t is TColumnName:
            if not isinstance(pv, str):
                raise DictValidationException(f"In {path}: field {pk} value {pv} has invalid type {type(pv).__name__} while str is expected", path, pk, pv)
            try:
                if naming.normalize_path(pv) != pv:
                    raise DictValidationException(f"In {path}: field {pk}: {pv} is not a valid column name", path, pk, pv)
            except ValueError:
                raise DictValidationException(f"In {path}: field {pk}: {pv} is not a valid column name", path, pk, pv)
            return True
        else:
            return False

    return validator


def _prepare_simple_regex(r: TSimpleRegex) -> str:
    if r.startswith(SIMPLE_REGEX_PREFIX):
        return r[3:]
    else:
        # exact matches
        return "^" + re.escape(r) + "$"


def compile_simple_regex(r: TSimpleRegex) -> REPattern:
    return re.compile(_prepare_simple_regex(r))


def compile_simple_regexes(r: Iterable[TSimpleRegex]) -> REPattern:
    """Compile multiple patterns as one"""
    pattern = '|'.join(f"({_prepare_simple_regex(p)})" for p in r)
    if not pattern:  # Don't create an empty pattern that matches everything
        raise ValueError("Cannot create a regex pattern from empty sequence")
    return re.compile(pattern)


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
    # return # dict(column)  # type: ignore
    return {
        **{  # type:ignore
            "nullable": True,
            "partition": False,
            "cluster": False,
            "unique": False,
            "sort": False,
            "primary_key": False,
            "foreign_key": False,
            "root_key": False,
            "merge_key": False
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


def is_complete_column(col: TColumnSchema) -> bool:
    """Returns true if column contains enough data to be created at the destination. Must contain a name and a data type. Other hints have defaults."""
    return bool(col.get("name")) and bool(col.get("data_type"))


def compare_complete_columns(a: TColumnSchema, b: TColumnSchema) -> bool:
    """Compares mandatory fields of complete columns"""
    assert is_complete_column(a)
    assert is_complete_column(b)
    return a["data_type"] == b["data_type"] and a["name"] == b["name"]


def merge_columns(col_a: TColumnSchema, col_b: TColumnSchema, merge_defaults: bool = False) -> TColumnSchema:
    """Merges `col_b` into `col_a`. if `merge_defaults` is True, only hints not present in `col_a` will be set."""
    # print(f"MERGE ({merge_defaults}) {col_b} into {col_a}")
    for n, v in col_b.items():
        if col_a.get(n) is None or not merge_defaults:
            col_a[n] = v  # type: ignore
    return col_a


def diff_tables(tab_a: TTableSchema, tab_b: TPartialTableSchema, ignore_table_name: bool = True) -> TPartialTableSchema:
    """Creates a partial table that contains properties found in `tab_b` that are not present in `tab_a` or that can be updated.
    Raises SchemaException if tables cannot be merged
    """
    table_name = tab_a["name"]
    if not ignore_table_name and table_name != tab_b["name"]:
        raise TablePropertiesConflictException(table_name, "name", table_name, tab_b["name"])

    # check if table properties can be merged
    if tab_a.get("parent") != tab_b.get("parent"):
        raise TablePropertiesConflictException(table_name, "parent", tab_a.get("parent"), tab_b.get("parent"))

    # get new columns, changes in the column data type or other properties are not allowed
    tab_a_columns = tab_a["columns"]
    new_columns: List[TColumnSchema] = []
    for col_b_name, col_b in tab_b["columns"].items():
        if col_b_name in tab_a_columns:
            col_a = tab_a_columns[col_b_name]
            # we do not support changing existing columns
            if is_complete_column(col_a) and is_complete_column(col_b):
                if not compare_complete_columns(tab_a_columns[col_b_name], col_b):
                    # attempt to update to incompatible columns
                    raise CannotCoerceColumnException(table_name, col_b_name, col_b["data_type"], tab_a_columns[col_b_name]["data_type"], None)
            # else:
            new_columns.append(merge_columns(col_a, col_b))
        else:
            new_columns.append(col_b)


    # return partial table containing only name and properties that differ (column, filters etc.)
    partial_table = new_table(table_name, columns=new_columns)
    partial_table["write_disposition"] = None
    # if tab_b.get("write_disposition")
    # partial_table["write_disposition"] = tab_b.get("write_disposition")
    # partial_table["description"] = tab_b.get("description")
    # partial_table["filters"] = deepcopy(tab_b.get("filters"))
    return partial_table



def compare_tables(tab_a: TTableSchema, tab_b: TTableSchema) -> bool:
    try:
        diff_table = diff_tables(tab_a, tab_b, ignore_table_name=False)
        # columns cannot differ
        return len(diff_table["columns"]) == 0
    except SchemaException:
        return False


def merge_tables(table: TTableSchema, partial_table: TPartialTableSchema) -> TPartialTableSchema:
    """Merges "partial_table" into "table", preserving the "table" name. Returns the diff partial table."""

    diff_table = diff_tables(table, partial_table, ignore_table_name=True)
    # add new columns when all checks passed
    table["columns"].update(diff_table["columns"])

    partial_w_d = partial_table.get("write_disposition")
    if partial_w_d:
        table["write_disposition"] = partial_w_d
    if table.get('parent') is None and (resource := partial_table.get('resource')):
        table['resource'] = resource

    return diff_table


def hint_to_column_prop(h: TColumnHint) -> TColumnProp:
    if h == "not_null":
        return "nullable"
    return h


def get_columns_names_with_prop(table: TTableSchema, column_prop: TColumnProp, only_completed: bool = False) -> List[str]:
    # column_prop: TColumnProp = hint_to_column_prop(hint_type)
    # default = column_prop != "nullable"  # default is true, only for nullable false
    return [c["name"] for c in table["columns"].values() if c.get(column_prop, False) is True and (not only_completed or is_complete_column(c))]


def merge_schema_updates(schema_updates: Sequence[TSchemaUpdate]) -> TSchemaTables:
    aggregated_update: TSchemaTables = {}
    for schema_update in schema_updates:
        for table_name, table_updates in schema_update.items():
            for partial_table in table_updates:
                # aggregate schema updates
                aggregated_table = aggregated_update.setdefault(table_name, partial_table)
                aggregated_table["columns"].update(partial_table["columns"])
    return aggregated_update


def get_write_disposition(tables: TSchemaTables, table_name: str) -> TWriteDisposition:
    """Returns write disposition of a table if present. If not, looks up into parent table"""
    table = tables[table_name]
    w_d = table.get("write_disposition")
    if w_d:
        return w_d

    parent = table.get("parent")
    if parent:
        return get_write_disposition(tables, parent)

    raise ValueError(f"No write disposition found in the chain of tables for '{table_name}'.")


def get_top_level_table(tables: TSchemaTables, table_name: str) -> TTableSchema:
    """Finds top level (without parent) of a `table_name` following the ancestry hierarchy."""
    table = tables[table_name]
    parent = table.get("parent")
    if parent:
        return get_top_level_table(tables, parent)
    return table


def get_child_tables(tables: TSchemaTables, table_name: str) -> List[TTableSchema]:
    """Get child tables for table name and return a list of tables ordered by ancestry so the child tables are always after their parents"""
    chain: List[TTableSchema] = []

    def _child(t: TTableSchema) -> None:
        name = t["name"]
        chain.append(t)
        for candidate in tables.values():
            if candidate.get("parent") == name:
                _child(candidate)

    _child(tables[table_name])
    return chain


def group_tables_by_resource(tables: TSchemaTables, pattern: Optional[REPattern] = None) -> Dict[str, List[TTableSchema]]:
    """Create a dict of resources and their associated tables and descendant tables
    If `pattern` is supplied, the result is filtered to only resource names matching the pattern.
    """
    result: Dict[str, List[TTableSchema]] = {}
    for table in tables.values():
        resource = table.get('resource')
        if resource and (pattern is None or pattern.match(resource)):
            resource_tables = result.setdefault(resource, [])
            resource_tables.extend(get_child_tables(tables, table['name']))
    return result


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
    validate_schema: bool = False,
    resource: str = None
) -> TTableSchema:

    table: TTableSchema = {
        "name": table_name,
        "columns": {} if columns is None else {c["name"]: add_missing_hints(c) for c in columns}
    }
    if parent_table_name:
        table["parent"] = parent_table_name
        assert write_disposition is None
        assert resource is None
    else:
        # set write disposition only for root tables
        table["write_disposition"] = write_disposition or DEFAULT_WRITE_DISPOSITION
        table["resource"] = resource or table_name
    if validate_schema:
        validate_dict(TTableSchema, table, f"new_table/{table_name}")
    return table


def new_column(column_name: str, data_type: TDataType = None, nullable: bool = True, validate_schema: bool = False) -> TColumnSchema:
    column = add_missing_hints({
                "name": column_name,
                "nullable": nullable
            })
    if data_type:
        column["data_type"] = data_type
    if validate_schema:
        validate_dict(TColumnSchema, column, f"new_column/{column_name}")
    return column


def standard_hints() -> Dict[TColumnHint, List[TSimpleRegex]]:
    return None


def standard_type_detections() -> List[TTypeDetections]:
    return ["timestamp", "iso_timestamp"]
