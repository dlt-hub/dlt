import re
import base64
import hashlib
import yaml
from copy import deepcopy, copy
from typing import Dict, List, Sequence, Tuple, Type, Any, cast, Iterable, Optional, Union

from dlt.common.pendulum import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.common import logger
from dlt.common.json import json
from dlt.common.data_types import TDataType
from dlt.common.exceptions import DictValidationException
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCase
from dlt.common.typing import DictStrAny, REPattern
from dlt.common.validation import TCustomValidator, validate_dict_ignoring_xkeys
from dlt.common.schema import detections
from dlt.common.schema.typing import (
    COLUMN_HINTS,
    SCHEMA_ENGINE_VERSION,
    LOADS_TABLE_NAME,
    SIMPLE_REGEX_PREFIX,
    VERSION_TABLE_NAME,
    PIPELINE_STATE_TABLE_NAME,
    TColumnName,
    TFileFormat,
    TPartialTableSchema,
    TSchemaTables,
    TSchemaUpdate,
    TSimpleRegex,
    TStoredSchema,
    TTableProcessingHints,
    TTableSchema,
    TColumnSchemaBase,
    TColumnSchema,
    TColumnProp,
    TTableFormat,
    TColumnHint,
    TTableSchemaColumns,
    TTypeDetectionFunc,
    TTypeDetections,
    TWriteDisposition,
    TLoaderMergeStrategy,
    TSchemaContract,
    TSortOrder,
)
from dlt.common.schema.exceptions import (
    CannotCoerceColumnException,
    ParentTableNotFoundException,
    TablePropertiesConflictException,
    InvalidSchemaName,
)


RE_NON_ALPHANUMERIC_UNDERSCORE = re.compile(r"[^a-zA-Z\d_]")
DEFAULT_WRITE_DISPOSITION: TWriteDisposition = "append"
DEFAULT_MERGE_STRATEGY: TLoaderMergeStrategy = "delete-insert"


def is_valid_schema_name(name: str) -> bool:
    """Schema name must be a valid python identifier and have max len of 64"""
    return (
        name is not None
        and name.isidentifier()
        and len(name) <= InvalidSchemaName.MAXIMUM_SCHEMA_NAME_LENGTH
    )


def normalize_schema_name(name: str) -> str:
    """Normalizes schema name by using snake case naming convention. The maximum length is 64 characters"""
    snake_case = SnakeCase(InvalidSchemaName.MAXIMUM_SCHEMA_NAME_LENGTH)
    return snake_case.normalize_identifier(name)


def apply_defaults(stored_schema: TStoredSchema) -> TStoredSchema:
    """Applies default hint values to `stored_schema` in place

    Updates only complete column hints, incomplete columns are preserved intact
    """
    for table_name, table in stored_schema["tables"].items():
        # overwrite name
        table["name"] = table_name
        # add default write disposition to root tables
        if table.get("parent") is None:
            if table.get("write_disposition") is None:
                table["write_disposition"] = DEFAULT_WRITE_DISPOSITION
            if table.get("resource") is None:
                table["resource"] = table_name
        for column_name in table["columns"]:
            # add default hints to tables
            column = table["columns"][column_name]
            # overwrite column name
            column["name"] = column_name
            # set column with default
            # table["columns"][column_name] = column
    return stored_schema


def remove_defaults(stored_schema: TStoredSchema) -> TStoredSchema:
    """Removes default values from `stored_schema` in place, returns the input for chaining

    * removes column and table names from the value
    * removed resource name if same as table name
    """
    clean_tables = deepcopy(stored_schema["tables"])
    for table_name, t in clean_tables.items():
        del t["name"]
        if t.get("resource") == table_name:
            del t["resource"]
        for c in t["columns"].values():
            # remove defaults only on complete columns
            # if is_complete_column(c):
            #     remove_column_defaults(c)
            #     # restore "nullable" True because we want to have explicit nullability in stored schema
            #     c["nullable"] = c.get("nullable", True)
            # do not save names
            del c["name"]

    stored_schema["tables"] = clean_tables
    return stored_schema


def has_default_column_prop_value(prop: str, value: Any) -> bool:
    """Checks if `value` is a default for `prop`."""
    # remove all boolean hints that are False, except "nullable" which is removed when it is True
    # TODO: merge column props and hints
    if prop in COLUMN_HINTS:
        return value in (False, None)
    # TODO: type all the hints including default value so those exceptions may be removed
    if prop == "nullable":
        return value in (True, None)
    if prop == "x-active-record-timestamp":
        # None is a valid value so it is not a default
        return False
    return value in (None, False)


def remove_column_defaults(column_schema: TColumnSchema) -> TColumnSchema:
    """Removes default values from `column_schema` in place, returns the input for chaining"""
    # remove hints with default values
    for h in list(column_schema.keys()):
        if has_default_column_prop_value(h, column_schema[h]):  # type: ignore
            del column_schema[h]  # type: ignore
    return column_schema


def bump_version_if_modified(stored_schema: TStoredSchema) -> Tuple[int, str, str, Sequence[str]]:
    """Bumps the `stored_schema` version and version hash if content modified, returns (new version, new hash, old hash, 10 last hashes) tuple"""
    hash_ = generate_version_hash(stored_schema)
    previous_hash = stored_schema.get("version_hash")
    previous_version = stored_schema.get("version")
    if not previous_hash:
        # if hash was not set, set it without bumping the version, that's initial schema
        # previous_version may not be None for migrating schemas
        stored_schema["version"] = previous_version or 1
    elif hash_ != previous_hash:
        stored_schema["version"] += 1
        store_prev_hash(stored_schema, previous_hash)

    stored_schema["version_hash"] = hash_
    return stored_schema["version"], hash_, previous_hash, stored_schema["previous_hashes"]


def store_prev_hash(
    stored_schema: TStoredSchema, previous_hash: str, max_history_len: int = 10
) -> None:
    # unshift previous hash to previous_hashes and limit array to 10 entries
    if previous_hash not in stored_schema["previous_hashes"]:
        stored_schema["previous_hashes"].insert(0, previous_hash)
        stored_schema["previous_hashes"] = stored_schema["previous_hashes"][:max_history_len]


def generate_version_hash(stored_schema: TStoredSchema) -> str:
    # generates hash out of stored schema content, excluding the hash itself and version
    schema_copy = copy(stored_schema)
    schema_copy.pop("version")
    schema_copy.pop("version_hash", None)
    schema_copy.pop("imported_version_hash", None)
    schema_copy.pop("previous_hashes", None)
    # ignore order of elements when computing the hash
    content = json.dumpb(schema_copy, sort_keys=True)
    h = hashlib.sha3_256(content)
    # additionally check column order
    table_names = sorted((schema_copy.get("tables") or {}).keys())
    if table_names:
        for tn in table_names:
            t = schema_copy["tables"][tn]
            h.update(tn.encode("utf-8"))
            # add column names to hash in order
            for cn in (t.get("columns") or {}).keys():
                h.update(cn.encode("utf-8"))
    return base64.b64encode(h.digest()).decode("ascii")


def verify_schema_hash(
    loaded_schema_dict: DictStrAny, verifies_if_not_migrated: bool = False
) -> bool:
    # generates content hash and compares with existing
    engine_version: str = loaded_schema_dict.get("engine_version")
    # if upgrade is needed, the hash cannot be compared
    if verifies_if_not_migrated and engine_version != SCHEMA_ENGINE_VERSION:
        return True
    # if hash is present we can assume at least v4 engine version so hash is computable
    stored_schema = cast(TStoredSchema, loaded_schema_dict)
    hash_ = generate_version_hash(stored_schema)
    return hash_ == stored_schema["version_hash"]


def normalize_simple_regex_column(naming: NamingConvention, regex: TSimpleRegex) -> TSimpleRegex:
    """Assumes that regex applies to column name and normalizes it."""

    def _normalize(r_: str) -> str:
        is_exact = len(r_) >= 2 and r_[0] == "^" and r_[-1] == "$"
        if is_exact:
            r_ = r_[1:-1]
        # if this a simple string then normalize it
        if r_ == re.escape(r_):
            r_ = naming.normalize_path(r_)
        if is_exact:
            r_ = "^" + r_ + "$"
        return r_

    if regex.startswith(SIMPLE_REGEX_PREFIX):
        return cast(TSimpleRegex, SIMPLE_REGEX_PREFIX + _normalize(regex[3:]))
    else:
        return cast(TSimpleRegex, _normalize(regex))


def canonical_simple_regex(regex: str) -> TSimpleRegex:
    if regex.startswith(SIMPLE_REGEX_PREFIX):
        return cast(TSimpleRegex, regex)
    else:
        return cast(TSimpleRegex, SIMPLE_REGEX_PREFIX + "^" + regex + "$")


def simple_regex_validator(path: str, pk: str, pv: Any, t: Any) -> bool:
    # custom validator on type TSimpleRegex
    if t is TSimpleRegex:
        if not isinstance(pv, str):
            raise DictValidationException(
                f"field {pk} value {pv} has invalid type {type(pv).__name__} while str is expected",
                path,
                t,
                pk,
                pv,
            )
        if pv.startswith(SIMPLE_REGEX_PREFIX):
            # check if regex
            try:
                re.compile(pv[3:])
            except Exception as e:
                raise DictValidationException(
                    f"field {pk} value {pv[3:]} does not compile as regex: {str(e)}",
                    path,
                    t,
                    pk,
                    pv,
                )
        else:
            if RE_NON_ALPHANUMERIC_UNDERSCORE.match(pv):
                raise DictValidationException(
                    f"field {pk} value {pv} looks like a regex, please prefix with re:",
                    path,
                    t,
                    pk,
                    pv,
                )
        # we know how to validate that type
        return True
    else:
        # don't know how to validate this
        return False


def column_name_validator(naming: NamingConvention) -> TCustomValidator:
    def validator(path: str, pk: str, pv: Any, t: Any) -> bool:
        if t is TColumnName:
            if not isinstance(pv, str):
                raise DictValidationException(
                    f"field {pk} value {pv} has invalid type {type(pv).__name__} while"
                    " str is expected",
                    path,
                    t,
                    pk,
                    pv,
                )
            try:
                if naming.normalize_path(pv) != pv:
                    raise DictValidationException(
                        f"field {pk}: {pv} is not a valid column name", path, t, pk, pv
                    )
            except ValueError:
                raise DictValidationException(
                    f"field {pk}: {pv} is not a valid column name", path, t, pk, pv
                )
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
    pattern = "|".join(f"({_prepare_simple_regex(p)})" for p in r)
    if not pattern:  # Don't create an empty pattern that matches everything
        raise ValueError("Cannot create a regex pattern from empty sequence")
    return re.compile(pattern)


def validate_stored_schema(stored_schema: TStoredSchema) -> None:
    # use lambda to verify only non extra fields
    validate_dict_ignoring_xkeys(
        spec=TStoredSchema, doc=stored_schema, path=".", validator_f=simple_regex_validator
    )
    # check child parent relationships
    for table_name, table in stored_schema["tables"].items():
        parent_table_name = table.get("parent")
        if parent_table_name:
            if parent_table_name not in stored_schema["tables"]:
                raise ParentTableNotFoundException(
                    stored_schema["name"], table_name, parent_table_name
                )


def autodetect_sc_type(detection_fs: Sequence[TTypeDetections], t: Type[Any], v: Any) -> TDataType:
    if detection_fs:
        for detection_fn in detection_fs:
            # the method must exist in the module
            detection_f: TTypeDetectionFunc = getattr(detections, "is_" + detection_fn)
            dt = detection_f(t, v)
            if dt is not None:
                return dt
    return None


def is_complete_column(col: TColumnSchemaBase) -> bool:
    """Returns true if column contains enough data to be created at the destination. Must contain a name and a data type. Other hints have defaults."""
    return bool(col.get("name")) and bool(col.get("data_type"))


def is_nullable_column(col: TColumnSchemaBase) -> bool:
    """Returns true if column is nullable"""
    return col.get("nullable", True)


def find_incomplete_columns(
    tables: List[TTableSchema],
) -> Iterable[Tuple[str, TColumnSchemaBase, bool]]:
    """Yields (table_name, column, nullable) for all incomplete columns in `tables`"""
    for table in tables:
        for col in table["columns"].values():
            if not is_complete_column(col):
                yield table["name"], col, is_nullable_column(col)


def compare_complete_columns(a: TColumnSchema, b: TColumnSchema) -> bool:
    """Compares mandatory fields of complete columns"""
    assert is_complete_column(a)
    assert is_complete_column(b)
    return a["data_type"] == b["data_type"] and a["name"] == b["name"]


def merge_column(
    col_a: TColumnSchema, col_b: TColumnSchema, merge_defaults: bool = True
) -> TColumnSchema:
    """Merges `col_b` into `col_a`. if `merge_defaults` is True, only hints from `col_b` that are not default in `col_a` will be set.

    Modifies col_a in place and returns it
    """
    col_b_clean = col_b if merge_defaults else remove_column_defaults(copy(col_b))
    for n, v in col_b_clean.items():
        col_a[n] = v  # type: ignore

    return col_a


def merge_columns(
    columns_a: TTableSchemaColumns,
    columns_b: TTableSchemaColumns,
    merge_columns: bool = False,
    columns_partial: bool = True,
) -> TTableSchemaColumns:
    """Merges `columns_a` with `columns_b`. `columns_a` is modified in place.

    * new columns are added
    * if `merge_columns` is False, updated columns are replaced from `columns_b`
    * if `merge_columns` is True, updated columns are merged with `merge_column`
    * if `columns_partial` is True, both columns sets are considered incomplete. In that case hints like `primary_key` or `merge_key` are merged
    * if `columns_partial` is False, hints like `primary_key` and `merge_key` are dropped from `columns_a` and replaced from `columns_b`
    * incomplete columns in `columns_a` that got completed in `columns_b` are removed to preserve order
    """
    if columns_partial is False:
        raise NotImplementedError("columns_partial must be False for merge_columns")

    # remove incomplete columns in table that are complete in diff table
    for col_name, column_b in columns_b.items():
        column_a = columns_a.get(col_name)
        if is_complete_column(column_b):
            if column_a and not is_complete_column(column_a):
                columns_a.pop(col_name)
        if column_a and merge_columns:
            column_b = merge_column(column_a, column_b)
        # set new or updated column
        columns_a[col_name] = column_b
    return columns_a


def diff_table(
    schema_name: str, tab_a: TTableSchema, tab_b: TPartialTableSchema
) -> TPartialTableSchema:
    """Creates a partial table that contains properties found in `tab_b` that are not present or different in `tab_a`.
    The name is always present in returned partial.
    It returns new columns (not present in tab_a) and merges columns from tab_b into tab_a (overriding non-default hint values).
    If any columns are returned they contain full data (not diffs of columns)

    Raises SchemaException if tables cannot be merged
    * when columns with the same name have different data types
    * when table links to different parent tables
    """
    table_name = tab_a["name"]
    # check if table properties can be merged
    if tab_a.get("parent") != tab_b.get("parent"):
        raise TablePropertiesConflictException(
            schema_name, table_name, "parent", tab_a.get("parent"), tab_b.get("parent")
        )

    # get new columns, changes in the column data type or other properties are not allowed
    tab_a_columns = tab_a["columns"]
    new_columns: List[TColumnSchema] = []
    for col_b_name, col_b in tab_b["columns"].items():
        if col_b_name in tab_a_columns:
            col_a = tab_a_columns[col_b_name]
            # we do not support changing data types of columns
            if is_complete_column(col_a) and is_complete_column(col_b):
                if not compare_complete_columns(tab_a_columns[col_b_name], col_b):
                    # attempt to update to incompatible columns
                    raise CannotCoerceColumnException(
                        schema_name,
                        table_name,
                        col_b_name,
                        col_b["data_type"],
                        tab_a_columns[col_b_name]["data_type"],
                        None,
                    )
            # all other properties can change
            merged_column = merge_column(copy(col_a), col_b)
            if merged_column != col_a:
                new_columns.append(merged_column)
        else:
            new_columns.append(col_b)

    # return partial table containing only name and properties that differ (column, filters etc.)
    partial_table: TPartialTableSchema = {
        "name": table_name,
        "columns": {} if new_columns is None else {c["name"]: c for c in new_columns},
    }
    for k, v in tab_b.items():
        if k in ["columns", None]:
            continue
        existing_v = tab_a.get(k)
        if existing_v != v:
            partial_table[k] = v  # type: ignore

    # this should not really happen
    if tab_a.get("parent") is not None and (resource := tab_b.get("resource")):
        raise TablePropertiesConflictException(
            schema_name, table_name, "resource", resource, tab_a.get("parent")
        )

    return partial_table


# def compare_tables(tab_a: TTableSchema, tab_b: TTableSchema) -> bool:
#     try:
#         table_name = tab_a["name"]
#         if table_name != tab_b["name"]:
#             raise TablePropertiesConflictException(table_name, "name", table_name, tab_b["name"])
#         diff_table = diff_tables(tab_a, tab_b, ignore_table_name=False)
#         # columns cannot differ
#         return len(diff_table["columns"]) == 0
#     except SchemaException:
#         return False


def merge_table(
    schema_name: str, table: TTableSchema, partial_table: TPartialTableSchema
) -> TPartialTableSchema:
    """Merges "partial_table" into "table". `table` is merged in place. Returns the diff partial table.

    `table` and `partial_table` names must be identical. A table diff is generated and applied to `table`:
    * new columns are added, updated columns are replaced from diff
    * incomplete columns in `table` that got completed in `partial_table` are removed to preserve order
    * table hints are added or replaced from diff
    * nothing gets deleted
    """

    if table["name"] != partial_table["name"]:
        raise TablePropertiesConflictException(
            schema_name, table["name"], "name", table["name"], partial_table["name"]
        )
    diff = diff_table(schema_name, table, partial_table)
    # add new columns when all checks passed
    updated_columns = merge_columns(table["columns"], diff["columns"])
    table.update(diff)
    table["columns"] = updated_columns

    return diff


def normalize_table_identifiers(table: TTableSchema, naming: NamingConvention) -> TTableSchema:
    """Normalizes all table and column names in `table` schema according to current schema naming convention and returns
    new instance with modified table schema.

    Naming convention like snake_case may produce name collisions with the column names. Colliding column schemas are merged
    where the column that is defined later in the dictionary overrides earlier column.

    Note that resource name is not normalized.
    """

    table = copy(table)
    table["name"] = naming.normalize_tables_path(table["name"])
    parent = table.get("parent")
    if parent:
        table["parent"] = naming.normalize_tables_path(parent)
    columns = table.get("columns")
    if columns:
        new_columns: TTableSchemaColumns = {}
        for c in columns.values():
            c = copy(c)
            origin_c_name = c["name"]
            new_col_name = c["name"] = naming.normalize_path(c["name"])
            # re-index columns as the name changed, if name space was reduced then
            # some columns now collide with each other. so make sure that we merge columns that are already there
            if new_col_name in new_columns:
                new_columns[new_col_name] = merge_column(
                    new_columns[new_col_name], c, merge_defaults=False
                )
                logger.warning(
                    f"In schema {naming} column {origin_c_name} got normalized into"
                    f" {new_col_name} which collides with other column. Both columns got merged"
                    " into one."
                )
            else:
                new_columns[new_col_name] = c
        table["columns"] = new_columns
    return table


def has_table_seen_data(table: TTableSchema) -> bool:
    """Checks if normalizer has seen data coming to the table."""
    return "x-normalizer" in table and table["x-normalizer"].get("seen-data", None) is True


def remove_processing_hints(tables: TSchemaTables) -> TSchemaTables:
    "Removes processing hints like x-normalizer and x-loader from schema tables. Modifies the input tables and returns it for convenience"
    for table_name, hints in get_processing_hints(tables).items():
        for hint in hints:
            del tables[table_name][hint]  # type: ignore[misc]
    return tables


def get_processing_hints(tables: TSchemaTables) -> Dict[str, List[str]]:
    """Finds processing hints in a set of tables and returns table_name: [hints] mapping"""
    hints: Dict[str, List[str]] = {}
    for table in tables.values():
        for hint in TTableProcessingHints.__annotations__.keys():
            if hint in table:
                table_hints = hints.setdefault(table["name"], [])
                table_hints.append(hint)
    return hints


def hint_to_column_prop(h: TColumnHint) -> TColumnProp:
    if h == "not_null":
        return "nullable"
    return h


def get_columns_names_with_prop(
    table: TTableSchema, column_prop: Union[TColumnProp, str], include_incomplete: bool = False
) -> List[str]:
    return [
        c["name"]
        for c in table["columns"].values()
        if column_prop in c
        and not has_default_column_prop_value(column_prop, c[column_prop])  # type: ignore[literal-required]
        and (include_incomplete or is_complete_column(c))
    ]


def get_first_column_name_with_prop(
    table: TTableSchema, column_prop: Union[TColumnProp, str], include_incomplete: bool = False
) -> Optional[str]:
    """Returns name of first column in `table` schema with property `column_prop` or None if no such column exists."""
    column_names = get_columns_names_with_prop(table, column_prop, include_incomplete)
    if len(column_names) > 0:
        return column_names[0]
    return None


def has_column_with_prop(
    table: TTableSchema, column_prop: Union[TColumnProp, str], include_incomplete: bool = False
) -> bool:
    """Checks if `table` schema contains column with property `column_prop`."""
    return len(get_columns_names_with_prop(table, column_prop, include_incomplete)) > 0


def get_dedup_sort_tuple(
    table: TTableSchema, include_incomplete: bool = False
) -> Optional[Tuple[str, TSortOrder]]:
    """Returns tuple with dedup sort information.

    First element is the sort column name, second element is the sort order.

    Returns None if "dedup_sort" hint was not provided.
    """
    dedup_sort_col = get_first_column_name_with_prop(table, "dedup_sort", include_incomplete)
    if dedup_sort_col is None:
        return None
    dedup_sort_order = table["columns"][dedup_sort_col]["dedup_sort"]
    return (dedup_sort_col, dedup_sort_order)


def get_validity_column_names(table: TTableSchema) -> List[Optional[str]]:
    return [
        get_first_column_name_with_prop(table, "x-valid-from"),
        get_first_column_name_with_prop(table, "x-valid-to"),
    ]


def get_active_record_timestamp(table: TTableSchema) -> Optional[pendulum.DateTime]:
    # method assumes a column with "x-active-record-timestamp" property exists
    cname = get_first_column_name_with_prop(table, "x-active-record-timestamp")
    hint_val = table["columns"][cname]["x-active-record-timestamp"]  # type: ignore[typeddict-item]
    return None if hint_val is None else ensure_pendulum_datetime(hint_val)


def merge_schema_updates(schema_updates: Sequence[TSchemaUpdate]) -> TSchemaTables:
    aggregated_update: TSchemaTables = {}
    for schema_update in schema_updates:
        for table_name, table_updates in schema_update.items():
            for partial_table in table_updates:
                # aggregate schema updates
                aggregated_table = aggregated_update.setdefault(table_name, partial_table)
                aggregated_table["columns"].update(partial_table["columns"])
    return aggregated_update


def get_inherited_table_hint(
    tables: TSchemaTables, table_name: str, table_hint_name: str, allow_none: bool = False
) -> Any:
    table = tables.get(table_name, {})
    hint = table.get(table_hint_name)
    if hint:
        return hint

    parent = table.get("parent")
    if parent:
        return get_inherited_table_hint(tables, parent, table_hint_name, allow_none)

    if allow_none:
        return None

    raise ValueError(
        f"No table hint '{table_hint_name} found in the chain of tables for '{table_name}'."
    )


def get_write_disposition(tables: TSchemaTables, table_name: str) -> TWriteDisposition:
    """Returns table hint of a table if present. If not, looks up into parent table"""
    return cast(
        TWriteDisposition,
        get_inherited_table_hint(tables, table_name, "write_disposition", allow_none=False),
    )


def get_table_format(tables: TSchemaTables, table_name: str) -> TTableFormat:
    return cast(
        TTableFormat, get_inherited_table_hint(tables, table_name, "table_format", allow_none=True)
    )


def get_file_format(tables: TSchemaTables, table_name: str) -> TFileFormat:
    return cast(
        TFileFormat, get_inherited_table_hint(tables, table_name, "file_format", allow_none=True)
    )


def get_merge_strategy(tables: TSchemaTables, table_name: str) -> TLoaderMergeStrategy:
    return cast(
        TLoaderMergeStrategy,
        get_inherited_table_hint(tables, table_name, "x-merge-strategy", allow_none=True),
    )


def fill_hints_from_parent_and_clone_table(
    tables: TSchemaTables, table: TTableSchema
) -> TTableSchema:
    """Takes write disposition and table format from parent tables if not present"""
    # make a copy of the schema so modifications do not affect the original document
    table = deepcopy(table)
    # add write disposition if not specified - in child tables
    if "write_disposition" not in table:
        table["write_disposition"] = get_write_disposition(tables, table["name"])
    if "table_format" not in table:
        table["table_format"] = get_table_format(tables, table["name"])
    if "file_format" not in table:
        table["file_format"] = get_file_format(tables, table["name"])
    return table


def table_schema_has_type(table: TTableSchema, _typ: TDataType) -> bool:
    """Checks if `table` schema contains column with type _typ"""
    return any(c.get("data_type") == _typ for c in table["columns"].values())


def table_schema_has_type_with_precision(table: TTableSchema, _typ: TDataType) -> bool:
    """Checks if `table` schema contains column with type _typ and precision set"""
    return any(
        c.get("data_type") == _typ and c.get("precision") is not None
        for c in table["columns"].values()
    )


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


def group_tables_by_resource(
    tables: TSchemaTables, pattern: Optional[REPattern] = None
) -> Dict[str, List[TTableSchema]]:
    """Create a dict of resources and their associated tables and descendant tables
    If `pattern` is supplied, the result is filtered to only resource names matching the pattern.
    """
    result: Dict[str, List[TTableSchema]] = {}
    for table in tables.values():
        resource = table.get("resource")
        if resource and (pattern is None or pattern.match(resource)):
            resource_tables = result.setdefault(resource, [])
            resource_tables.extend(get_child_tables(tables, table["name"]))
    return result


def version_table() -> TTableSchema:
    # NOTE: always add new columns at the end of the table so we have identical layout
    # after an update of existing tables (always at the end)
    # set to nullable so we can migrate existing tables
    # WARNING: do not reorder the columns
    table = new_table(
        VERSION_TABLE_NAME,
        columns=[
            {
                "name": "version",
                "data_type": "bigint",
                "nullable": False,
            },
            {"name": "engine_version", "data_type": "bigint", "nullable": False},
            {"name": "inserted_at", "data_type": "timestamp", "nullable": False},
            {"name": "schema_name", "data_type": "text", "nullable": False},
            {"name": "version_hash", "data_type": "text", "nullable": False},
            {"name": "schema", "data_type": "text", "nullable": False},
        ],
    )
    table["write_disposition"] = "skip"
    table["description"] = "Created by DLT. Tracks schema updates"
    return table


def loads_table() -> TTableSchema:
    # NOTE: always add new columns at the end of the table so we have identical layout
    # after an update of existing tables (always at the end)
    # set to nullable so we can migrate existing tables
    # WARNING: do not reorder the columns
    table = new_table(
        LOADS_TABLE_NAME,
        columns=[
            {"name": "load_id", "data_type": "text", "nullable": False},
            {"name": "schema_name", "data_type": "text", "nullable": True},
            {"name": "status", "data_type": "bigint", "nullable": False},
            {"name": "inserted_at", "data_type": "timestamp", "nullable": False},
            {
                "name": "schema_version_hash",
                "data_type": "text",
                "nullable": True,
            },
        ],
    )
    table["write_disposition"] = "skip"
    table["description"] = "Created by DLT. Tracks completed loads"
    return table


def pipeline_state_table(add_dlt_id: bool = False) -> TTableSchema:
    # NOTE: always add new columns at the end of the table so we have identical layout
    # after an update of existing tables (always at the end)
    # set to nullable so we can migrate existing tables
    # WARNING: do not reorder the columns
    columns: List[TColumnSchema] = [
        {"name": "version", "data_type": "bigint", "nullable": False},
        {"name": "engine_version", "data_type": "bigint", "nullable": False},
        {"name": "pipeline_name", "data_type": "text", "nullable": False},
        {"name": "state", "data_type": "text", "nullable": False},
        {"name": "created_at", "data_type": "timestamp", "nullable": False},
        {"name": "version_hash", "data_type": "text", "nullable": True},
        {"name": "_dlt_load_id", "data_type": "text", "nullable": False},
    ]
    if add_dlt_id:
        columns.append({"name": "_dlt_id", "data_type": "text", "nullable": False, "unique": True})
    table = new_table(
        PIPELINE_STATE_TABLE_NAME,
        write_disposition="append",
        columns=columns,
        # always use caps preferred file format for processing
        file_format="preferred",
    )
    table["description"] = "Created by DLT. Tracks pipeline state"
    return table


def new_table(
    table_name: str,
    parent_table_name: str = None,
    write_disposition: TWriteDisposition = None,
    columns: Sequence[TColumnSchema] = None,
    validate_schema: bool = False,
    resource: str = None,
    schema_contract: TSchemaContract = None,
    table_format: TTableFormat = None,
    file_format: TFileFormat = None,
) -> TTableSchema:
    table: TTableSchema = {
        "name": table_name,
        "columns": {} if columns is None else {c["name"]: c for c in columns},
    }
    if parent_table_name:
        table["parent"] = parent_table_name
        assert write_disposition is None
        assert resource is None
        assert schema_contract is None
    else:
        # set write disposition only for root tables
        table["write_disposition"] = write_disposition or DEFAULT_WRITE_DISPOSITION
        table["resource"] = resource or table_name
        if schema_contract is not None:
            table["schema_contract"] = schema_contract
        if table_format:
            table["table_format"] = table_format
        if file_format:
            table["file_format"] = file_format
    if validate_schema:
        validate_dict_ignoring_xkeys(
            spec=TColumnSchema,
            doc=table["columns"],
            path=f"new_table/{table_name}",
        )

    return table


def new_column(
    column_name: str,
    data_type: TDataType = None,
    nullable: bool = True,
    precision: int = None,
    scale: int = None,
    validate_schema: bool = False,
) -> TColumnSchema:
    column: TColumnSchema = {"name": column_name, "nullable": nullable}
    if data_type:
        column["data_type"] = data_type
    if precision is not None:
        column["precision"] = precision
    if scale is not None:
        column["scale"] = scale
    if validate_schema:
        validate_dict_ignoring_xkeys(
            spec=TColumnSchema,
            doc=column,
            path=f"new_column/{column_name}",
        )

    return column


def default_hints() -> Dict[TColumnHint, List[TSimpleRegex]]:
    return None


def standard_type_detections() -> List[TTypeDetections]:
    return ["iso_timestamp"]


def to_pretty_json(stored_schema: TStoredSchema) -> str:
    return json.dumps(stored_schema, pretty=True)


def to_pretty_yaml(stored_schema: TStoredSchema) -> str:
    return yaml.dump(stored_schema, allow_unicode=True, default_flow_style=False, sort_keys=False)
