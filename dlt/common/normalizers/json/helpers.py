"""
Cached helper methods for all operations that are called often
"""
from functools import lru_cache
from typing import Any, Collection, Dict, List, Optional, Tuple, cast

from dlt.common import logger
from dlt.common.json import json
from dlt.common.destination.utils import resolve_merge_strategy
from dlt.common.normalizers.json.typing import RelationalNormalizerConfigPropagation
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.normalizers.typing import TRowIdType
from dlt.common.normalizers.utils import DLT_ID_LENGTH_BYTES
from dlt.common.schema import Schema
from dlt.common.schema.typing import (
    TColumnName,
    TColumnSchema,
    C_DLT_ID,
    C_DLT_LOAD_ID,
    DLT_NAME_PREFIX,
    TTableSchema,
)
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_first_column_name_with_prop,
    get_nested_tables,
    has_column_with_prop,
    is_complete_column,
    is_nested_table,
    is_nullable_column,
)
from dlt.common.utils import digest128, digest128b

MIX_BACKENDS_DOCS_URL = (
    "https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas"
    "#switch-between-arrow-tables-and-python-objects"
)
SCD2_DOCS_URL = "https://dlthub.com/docs/general-usage/merge-loading#scd2-strategy"


def shorten_fragments(naming: NamingConvention, *idents: str) -> str:
    return naming.shorten_fragments(*idents)


def normalize_table_identifier(schema: Schema, naming: NamingConvention, table_name: str) -> str:
    if schema._normalizers_config.get("use_break_path_on_normalize", True):
        return naming.normalize_tables_path(table_name)
    else:
        return naming.normalize_table_identifier(table_name)


def normalize_identifier(schema: Schema, naming: NamingConvention, identifier: str) -> str:
    if schema._normalizers_config.get("use_break_path_on_normalize", True):
        return naming.normalize_path(identifier)
    else:
        return naming.normalize_identifier(identifier)


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


def get_primary_key(schema: Schema, table_name: str) -> List[str]:
    if table_name not in schema.tables:
        return []
    table = schema.get_table(table_name)
    pk = get_columns_names_with_prop(table, "primary_key", include_incomplete=True)
    return pk


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


def should_be_nested(schema: Schema, table_name: str) -> bool:
    """Tells if table should be nested or should be a root table. All tables created by the normalizer
    are nested, defined tables are checked.
    """
    if table := schema.tables.get(table_name):
        return is_nested_table(table)
    return True


def get_root_row_id_type(schema: Schema, table_name: str) -> TRowIdType:
    if table := schema.tables.get(table_name):
        merge_strategy = resolve_merge_strategy(schema.tables, table, is_prepared=False)
        if merge_strategy in ("upsert", "insert-only", "cdc"):
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


def get_dlt_columns(schema: Schema, table: TTableSchema) -> List[str]:
    """Returns `_dlt_load_id` and the row id column of `table`: the `row_key` column or the scd2
    row version column that holds the row hash.
    """
    row_id_col = get_first_column_name_with_prop(
        table, "row_key"
    ) or get_first_column_name_with_prop(table, "x-row-version")
    dlt_columns = [schema.naming.normalize_identifier(C_DLT_LOAD_ID), row_id_col]
    return [name for name in dlt_columns if name in table["columns"]]


def verify_arrow_dlt_columns(
    schema: Schema, table_name: str, arrow_column_names: Collection[str]
) -> None:
    """Warns if NOT NULL `_dlt_load_id` or row id column of `table_name` gets no data from arrow
    tables with `arrow_column_names` columns, or if the scd2 row hash in `_dlt_id` is missing.
    """
    table = schema.tables.get(table_name)
    if not table:
        return
    missing_dlt_columns = [
        name
        for name in get_dlt_columns(schema, table)
        if name not in arrow_column_names
        and is_complete_column(table["columns"][name])
        and not is_nullable_column(table["columns"][name])
    ]
    dlt_id_col = schema.naming.normalize_identifier(C_DLT_ID)
    if (
        dlt_id_col not in arrow_column_names
        and get_root_row_id_type(schema, table_name) == "row_hash"
    ):
        logger.warning(
            f"Table {table_name} uses the scd2 merge strategy that compares row hashes in"
            f" `{dlt_id_col}` to find changed rows, but arrow tables and data frames do not get"
            " this column by default. The load will fail. Set `add_dlt_id = true` in"
            " `[normalize.parquet_normalizer]` so dlt hashes each row (row by row, like for"
            " Python objects) or add your own row hash column (ie. with `add_row_hash_to_table`"
            " from `dlt.sources.helpers.transform`) and pass its name in"
            f" `row_version_column_name`. See {SCD2_DOCS_URL}"
        )
        missing_dlt_columns = [name for name in missing_dlt_columns if name != dlt_id_col]
    if not missing_dlt_columns:
        return
    load_id_col = schema.naming.normalize_identifier(C_DLT_LOAD_ID)
    settings = [
        "`add_dlt_load_id`" if name == load_id_col else "`add_dlt_id`"
        for name in missing_dlt_columns
    ]
    logger.warning(
        f"Table {table_name} has NOT NULL columns {missing_dlt_columns} that arrow tables in this"
        " load do not have. This happens when a resource that yielded Python objects starts to"
        ' yield arrow tables or data frames (ie. `backend="pyarrow"` in `sql_database`): Python'
        " objects get these columns, arrow tables do not by default. The load fails on destinations"
        f" that enforce NOT NULL. Set {' and '.join(settings)} in"
        " `[normalize.parquet_normalizer]` to add the columns to arrow tables (this adds compute"
        " and storage cost for every row) or drop the table and its state with"
        f' `refresh="drop_resources"` and load again. See {MIX_BACKENDS_DOCS_URL}'
    )


def get_propagation_mapping(
    config: RelationalNormalizerConfigPropagation, table: str, is_root: bool
) -> Dict[TColumnName, TColumnName]:
    # mapping(k:v): propagate property with name "k" as property with name "v" in nested table
    mappings = {}
    if is_root:
        mappings.update(config.get("root") or {})
    if table in (config.get("tables") or {}):
        mappings.update(config["tables"][table])
    return mappings


def requires_root_key(
    schema: Schema, root_table: TTableSchema, root_key_propagation: Optional[bool]
) -> bool:
    """Checks if table chain containing `table` requires root_key to propagate.

    1. if there's any table with `root_key` in table chain already - we always propagate
    2. if merge strategy is "delete-insert", "upsert", "insert-only" or "cdc" and root_key_propagation is None
    3. root_key_propagation is True
    """
    table_name = root_table["name"]
    assert not is_nested_table(root_table), f"{table_name} cannot be nested table"
    if table_name in schema.dlt_table_names():
        merge_requires = False
    else:
        merge_strategy = resolve_merge_strategy(schema.tables, root_table, is_prepared=False)
        # scd2 never deletes nested rows and inserts them by row key, so it does not need root key
        merge_requires = (
            merge_strategy in ["delete-insert", "upsert", "insert-only", "cdc"]
            if root_key_propagation is None
            else root_key_propagation
        )
    return merge_requires or any(
        has_column_with_prop(t, "root_key", include_incomplete=True)
        for t in get_nested_tables(schema.tables, table_name)
    )


def get_row_hash(row: Dict[str, Any], subset: Optional[List[str]] = None) -> str:
    """Returns hash of row.

    Hash includes column names and values and is ordered by column name.
    Excludes dlt system columns.
    Can be used as deterministic row identifier.
    """
    if subset is not None:
        # all parts of the key must be present
        row_filtered = {k: row[k] for k in subset}
    else:
        row_filtered = {k: v for k, v in row.items() if not k.startswith(DLT_NAME_PREFIX)}
    row_str = json.dumpb(row_filtered, sort_keys=True)
    return digest128b(row_str, DLT_ID_LENGTH_BYTES)


def get_nested_row_hash(parent_row_id: str, nested_table: str, list_idx: int) -> str:
    # create deterministic unique id of the nested row taking into account that all lists are ordered
    # and all nested tables must be lists
    return digest128(f"{parent_row_id}_{nested_table}_{list_idx}", DLT_ID_LENGTH_BYTES)
