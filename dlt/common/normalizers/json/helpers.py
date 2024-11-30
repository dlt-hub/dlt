"""
Cached helper methods for all operations that are called often
"""
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple, cast

from dlt.common.json import json
from dlt.common.destination.utils import resolve_merge_strategy
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.normalizers.typing import TRowIdType
from dlt.common.normalizers.utils import DLT_ID_LENGTH_BYTES
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, C_DLT_ID, DLT_NAME_PREFIX
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_first_column_name_with_prop,
    is_nested_table,
)
from dlt.common.utils import digest128


@lru_cache(maxsize=None)
def shorten_fragments(naming: NamingConvention, *idents: str) -> str:
    return naming.shorten_fragments(*idents)


@lru_cache(maxsize=None)
def normalize_table_identifier(schema: Schema, naming: NamingConvention, table_name: str) -> str:
    if schema._normalizers_config.get("use_break_path_on_normalize", True):
        return naming.normalize_tables_path(table_name)
    else:
        return naming.normalize_table_identifier(table_name)


@lru_cache(maxsize=None)
def normalize_identifier(schema: Schema, naming: NamingConvention, identifier: str) -> str:
    if schema._normalizers_config.get("use_break_path_on_normalize", True):
        return naming.normalize_path(identifier)
    else:
        return naming.normalize_identifier(identifier)


@lru_cache(maxsize=None)
def get_table_nesting_level(
    schema: Schema, table_name: str, default_nesting: int = 1000
) -> Optional[int]:
    """gets table nesting level, will inherit from parent if not set"""

    table = schema.tables.get(table_name)
    if (
        table
        and (max_nesting := cast(int, table.get("x-normalizer", {}).get("max_nesting"))) is not None
    ):
        return max_nesting
    return default_nesting


@lru_cache(maxsize=None)
def get_primary_key(schema: Schema, table_name: str) -> List[str]:
    if table_name not in schema.tables:
        return []
    table = schema.get_table(table_name)
    return get_columns_names_with_prop(table, "primary_key", include_incomplete=True)


@lru_cache(maxsize=None)
def is_nested_type(
    schema: Schema,
    table_name: str,
    field_name: str,
    _r_lvl: int,
) -> bool:
    """For those paths the nested objects should be left in place.
    Cache perf: max_nesting < _r_lvl: ~2x faster, full check 10x faster
    """

    # nesting level is counted backwards
    # is we have traversed to or beyond the calculated nesting level, we detect a nested type
    if _r_lvl <= 0:
        return True

    column: TColumnSchema = None
    table = schema.tables.get(table_name)
    if table:
        column = table["columns"].get(field_name)
    if column is None or "data_type" not in column:
        data_type = schema.get_preferred_type(field_name)
    else:
        data_type = column["data_type"]

    return data_type == "json"


@lru_cache(maxsize=None)
def get_nested_row_id_type(schema: Schema, table_name: str) -> Tuple[TRowIdType, bool]:
    """Gets type of row id to be added to nested table and if linking information should be added"""
    if table := schema.tables.get(table_name):
        merge_strategy = resolve_merge_strategy(schema.tables, table)
        if merge_strategy not in ("upsert", "scd2") and not is_nested_table(table):
            return "random", False
    else:
        # table will be created, use standard linking
        pass
    return "row_hash", True


@lru_cache(maxsize=None)
def get_root_row_id_type(schema: Schema, table_name: str) -> TRowIdType:
    if table := schema.tables.get(table_name):
        merge_strategy = resolve_merge_strategy(schema.tables, table)
        if merge_strategy == "upsert":
            return "key_hash"
        elif merge_strategy == "scd2":
            x_row_version_col = get_first_column_name_with_prop(
                schema.get_table(table_name),
                "x-row-version",
                include_incomplete=True,
            )
            if x_row_version_col == schema.naming.normalize_identifier(C_DLT_ID):
                return "row_hash"
    return "random"


def get_row_hash(row: Dict[str, Any], subset: Optional[List[str]] = None) -> str:
    """Returns hash of row.

    Hash includes column names and values and is ordered by column name.
    Excludes dlt system columns.
    Can be used as deterministic row identifier.
    """
    row_filtered = {k: v for k, v in row.items() if not k.startswith(DLT_NAME_PREFIX)}
    if subset is not None:
        row_filtered = {k: v for k, v in row.items() if k in subset}
    row_str = json.dumps(row_filtered, sort_keys=True)
    return digest128(row_str, DLT_ID_LENGTH_BYTES)


def get_nested_row_hash(parent_row_id: str, nested_table: str, list_idx: int) -> str:
    # create deterministic unique id of the nested row taking into account that all lists are ordered
    # and all nested tables must be lists
    return digest128(f"{parent_row_id}_{nested_table}_{list_idx}", DLT_ID_LENGTH_BYTES)
