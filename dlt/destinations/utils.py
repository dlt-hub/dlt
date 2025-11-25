import re

from typing import Any, List, Dict, Type, Optional, Sequence, Tuple, cast, Iterable, Callable

from sqlglot import column

from dlt.common import logger
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.destination.utils import resolve_merge_strategy, resolve_replace_strategy
from dlt.common.destination.client import WithTableReflection
from dlt.common.schema import Schema, TSchemaDrop
from dlt.common.schema.exceptions import SchemaCorruptedException
from dlt.common.schema.typing import (
    MERGE_STRATEGIES,
    TColumnSchema,
    TLoaderReplaceStrategy,
    TTableSchema,
    TPartialTableSchema,
)
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_first_column_name_with_prop,
    has_column_with_prop,
    is_nested_table,
    pipeline_state_table,
)

from dlt.destinations.exceptions import DatabaseTransientException
from dlt.destinations.sql_client import WithSqlClient
from dlt.extract import DltResource, resource as make_resource, DltSource

RE_DATA_TYPE = re.compile(r"([A-Z]+)\((\d+)(?:,\s?(\d+))?\)")


def get_resource_for_adapter(data: Any) -> DltResource:
    """
    Helper function for adapters. Wraps `data` in a DltResource if it's not a DltResource already.
    Alternatively if `data` is a DltSource, throws an error if there are multiple resource in the source
    or returns the single resource if available.
    """
    if isinstance(data, DltResource):
        return data
    # prevent accidentally wrapping sources with adapters
    if isinstance(data, DltSource):
        if len(data.selected_resources.keys()) == 1:
            return list(data.selected_resources.values())[0]
        else:
            raise ValueError(
                "You are trying to use an adapter on a `DltSource` with multiple resources. You can"
                " only use adapters on: pure data, a `DltResouce` or a `DltSource` with a single"
                " `DltResource`."
            )

    resource_name = None
    if not hasattr(data, "__name__"):
        logger.info("Setting default resource name to `content` for adapted resource.")
        resource_name = "content"

    return cast(DltResource, make_resource(data, name=resource_name))


def info_schema_null_to_bool(v: str) -> bool:
    """Converts INFORMATION SCHEMA truth values to Python bool"""
    if v in ("NO", "0"):
        return False
    elif v in ("YES", "1"):
        return True
    raise ValueError(v)


def parse_db_data_type_str_with_precision(db_type: str) -> Tuple[str, Optional[int], Optional[int]]:
    """Parses a db data type with optional precision or precision and scale information"""
    # Search for matches using the regular expression
    match = RE_DATA_TYPE.match(db_type)

    # If the pattern matches, extract the type, precision, and scale
    if match:
        db_type = match.group(1)
        precision = int(match.group(2))
        scale = int(match.group(3)) if match.group(3) else None
        return db_type, precision, scale

    # If the pattern does not match, return the original type without precision and scale
    return db_type, None, None


def get_pipeline_state_query_columns() -> TTableSchema:
    """We get definition of pipeline state table without columns we do not need for the query"""
    state_table = pipeline_state_table()
    # we do not need version_hash to be backward compatible as long as we can
    state_table["columns"].pop("version_hash")
    return state_table


def verify_schema_replace_disposition(
    schema: Schema,
    load_tables: Sequence[PreparedTableSchema],
    capabilities: DestinationCapabilitiesContext,
    required_strategy: TLoaderReplaceStrategy,
    warnings: bool = True,
) -> List[Exception]:
    # log = logger.warning if warnings else logger.info
    # collect all exceptions to show all problems in the schema
    exception_log: List[Exception] = []

    # verifies schema settings specific to sql job client
    for table in load_tables:
        # from now on validate only top level tables
        if is_nested_table(table):
            continue
        table_name = table["name"]
        if table["write_disposition"] == "replace":
            if not resolve_replace_strategy(table, required_strategy, capabilities):
                format_info = ""
                if capabilities.supported_table_formats:
                    format_info = (
                        " Note that setting table format may allow you to pick other replace"
                        " strategies. Available formats:"
                        f" {', '.join(capabilities.supported_table_formats)}"
                    )
                exception_log.append(
                    # Pick one of: {', '.join(supported_replace_strategies)} .
                    SchemaCorruptedException(
                        schema.name,
                        f"Requested replace strategy {required_strategy} not available for table"
                        f" {table_name}. "
                        + format_info,
                    )
                )
    return exception_log


def verify_schema_merge_disposition(
    schema: Schema,
    load_tables: Sequence[PreparedTableSchema],
    capabilities: DestinationCapabilitiesContext,
    warnings: bool = True,
) -> List[Exception]:
    log = logger.warning if warnings else logger.info
    # collect all exceptions to show all problems in the schema
    exception_log: List[Exception] = []

    # verifies schema settings specific to sql job client
    for table in load_tables:
        # from now on validate only top level tables
        if is_nested_table(table):
            continue

        table_name = table["name"]
        if table["write_disposition"] == "merge":
            if "x-merge-strategy" in table and table["x-merge-strategy"] not in MERGE_STRATEGIES:  # type: ignore[typeddict-item]
                exception_log.append(
                    SchemaCorruptedException(
                        schema.name,
                        f'"{table["x-merge-strategy"]}" is not a valid merge strategy. '  # type: ignore[typeddict-item]
                        f"""Allowed values: {', '.join(['"' + s + '"' for s in MERGE_STRATEGIES])}.""",
                    )
                )

            merge_strategy = resolve_merge_strategy(schema.tables, table, capabilities)
            if merge_strategy == "delete-insert":
                if not has_column_with_prop(table, "primary_key") and not has_column_with_prop(
                    table, "merge_key"
                ):
                    log(
                        f"Table {table_name} has `write_disposition` set to `merge`"
                        " and `merge_strategy` set to `delete-insert`, but no primary or"
                        " merge keys defined."
                        " dlt will fall back to `append` for this table."
                    )
            elif merge_strategy == "upsert":
                if not has_column_with_prop(table, "primary_key"):
                    exception_log.append(
                        SchemaCorruptedException(
                            schema.name,
                            f"No primary key defined for table `{table['name']}`."
                            " `primary_key` needs to be set when using the `upsert`"
                            " merge strategy.",
                        )
                    )
                if has_column_with_prop(table, "merge_key"):
                    log(
                        f"Found `merge_key` for table `{table['name']}` with"
                        " `upsert` merge strategy. Merge key is not supported"
                        " for this strategy and will be ignored."
                    )
        if has_column_with_prop(table, "hard_delete"):
            if len(get_columns_names_with_prop(table, "hard_delete")) > 1:
                exception_log.append(
                    SchemaCorruptedException(
                        schema.name,
                        f'Found multiple "hard_delete" column hints for table "{table_name}" in'
                        f' schema "{schema.name}" while only one is allowed:'
                        f' {", ".join(get_columns_names_with_prop(table, "hard_delete"))}.',
                    )
                )
            if table.get("write_disposition") in ("replace", "append"):
                log(
                    f"""The "hard_delete" column hint for column "{get_first_column_name_with_prop(table, 'hard_delete')}" """
                    f'in table "{table_name}" with write disposition'
                    f' "{table.get("write_disposition")}"'
                    f' in schema "{schema.name}" will be ignored.'
                    ' The "hard_delete" column hint is only applied when using'
                    ' the "merge" write disposition.'
                )
        if has_column_with_prop(table, "dedup_sort"):
            if len(get_columns_names_with_prop(table, "dedup_sort")) > 1:
                exception_log.append(
                    SchemaCorruptedException(
                        schema.name,
                        f'Found multiple "dedup_sort" column hints for table "{table_name}" in'
                        f' schema "{schema.name}" while only one is allowed:'
                        f' {", ".join(get_columns_names_with_prop(table, "dedup_sort"))}.',
                    )
                )
            if table.get("write_disposition") in ("replace", "append"):
                log(
                    f"""The "dedup_sort" column hint for column "{get_first_column_name_with_prop(table, 'dedup_sort')}" """
                    f'in table "{table_name}" with write disposition'
                    f' "{table.get("write_disposition")}"'
                    f' in schema "{schema.name}" will be ignored.'
                    ' The "dedup_sort" column hint is only applied when using'
                    ' the "merge" write disposition.'
                )
            if table.get("write_disposition") == "merge":
                if not has_column_with_prop(table, "primary_key"):
                    log(
                        f"""The "dedup_sort" column hint for column "{get_first_column_name_with_prop(table, 'dedup_sort')}" """
                        f'in table "{table_name}" with write disposition'
                        f' "{table.get("write_disposition")}"'
                        f' in schema "{schema.name}" will be ignored.'
                        ' The "dedup_sort" column hint is only applied when a'
                        " primary key has been specified."
                    )
                if table.get("x-stage-data-deduplicated", False):
                    log(
                        f"""The "dedup_sort" column hint for column "{get_first_column_name_with_prop(table, 'dedup_sort')}" """
                        f'in table "{table_name}" with write disposition'
                        f' "{table.get("write_disposition")}"'
                        f' in schema "{schema.name}" will be ignored.'
                        " Staging data is declared as already deduplicated."
                    )
    return exception_log


def _convert_to_old_pyformat(
    new_style_string: str, args: Tuple[Any, ...], operational_error_cls: Type[Exception]
) -> Tuple[str, Dict[str, Any]]:
    """Converts a query string from the new pyformat style to the old pyformat style.

    The new pyformat style uses placeholders like %s, while the old pyformat style
    uses placeholders like %(arg0)s, where the number corresponds to the index of
    the argument in the args tuple.

    Args:
        new_style_string (str): The query string in the new pyformat style.
        args (Tuple[Any, ...]): The arguments to be inserted into the query string.
        operational_error_cls (Type[Exception]): The specific OperationalError class to be raised
            in case of a mismatch between placeholders and arguments. This should be the
            OperationalError class provided by the DBAPI2-compliant driver being used.

    Returns:
        Tuple[str, Dict[str, Any]]: A tuple containing the converted query string
            in the old pyformat style, and a dictionary mapping argument keys to values.

    Raises:
        DatabaseTransientException: If there is a mismatch between the number of
            placeholders in the query string, and the number of arguments provided.
    """
    if len(args) == 1 and isinstance(args[0], tuple):
        args = args[0]

    keys = [f"arg{str(i)}" for i, _ in enumerate(args)]
    old_style_string, count = re.subn(r"%s", lambda _: f"%({keys.pop(0)})s", new_style_string)
    mapping = dict(zip([f"arg{str(i)}" for i, _ in enumerate(args)], args))
    if count != len(args):
        raise DatabaseTransientException(operational_error_cls())
    return old_style_string, mapping


def is_compression_disabled() -> bool:
    from dlt import config

    key_ = "normalize.data_writer.disable_compression"
    if key_ not in config:
        key_ = "data_writer.disable_compression"
    return config.get(key_, bool)


def get_deterministic_temp_table_name(table_name: str, op: str) -> str:
    """Returns table name suitable for deterministic temp table. Such table will survive
    disconnects and is supposed to be dropped before being filled again so we need
    deterministic name with very low collision prob with "data" tables
    """
    from dlt.common.normalizers.naming import NamingConvention

    op_name = f"{table_name}_{op}"
    return f"{op_name}_{NamingConvention._compute_tag(op_name, 0.001)}"


class WithTableReflectionAndSql(WithTableReflection, WithSqlClient):
    pass


def sync_schema_from_storage_schema(
    get_storage_tables_f: Callable[[Iterable[str]], Iterable[tuple[str, dict[str, TColumnSchema]]]],
    escape_col_f: Callable[[str, bool, bool], str],
    schema: Schema,
    table_names: Iterable[str] = None,
    dry_run: bool = False,
) -> Optional[TSchemaDrop]:
    """Updates the dlt schema from destination.

    Compare the schema we think we should have with what actually exists in the destination,
    and drop any tables and/or columns that disappeared.

    Args:
        client (WithTableReflectionAndSql): The destination client with table reflection capabilities.
        schema (Schema): The dlt schema to compare against the destination.
        table_names (Iterable[str], optional): Check only listed tables. Defaults to None and checks all tables.
        dry_run (bool, optional): Whether to actually update the dlt schema. Defaults to False.

    Returns:
        Optional[TSchemaDrop]: Returns the update that was applied to the schema.
    """
    tables = table_names if table_names else schema.data_table_names()

    table_drops: TSchemaDrop = {}  # includes entire tables to drop
    column_drops: TSchemaDrop = {}  # includes parts of tables to drop as partial tables

    # 1. Detect what needs to be dropped
    actual_table_col_schemas = dict(get_storage_tables_f(tables))
    for table_name in tables:
        actual_col_schemas = actual_table_col_schemas[table_name]

        # no actual column schemas ->
        # table doesn't exist ->
        # we take entire table schema as a schema drop
        if not actual_col_schemas:
            table = schema.get_table(table_name)
            table_drops[table_name] = table
            continue

        # actual column schemas present ->
        # we compare actual column schemas with dlt ones ->
        # we take the difference as a partial table
        else:
            removed_columns = schema.get_removed_table_columns(
                table_name=table_name,
                existing_columns=actual_col_schemas,
                escape_col_f=escape_col_f,
                disregard_dlt_columns=True,
            )
            if removed_columns:
                partial_table: TPartialTableSchema = {
                    "name": table_name,
                    "columns": {col["name"]: col for col in removed_columns},
                }
                column_drops[table_name] = partial_table

    # 2. For entire table drops, we make sure no orphaned tables remain
    for table_name in table_drops.copy():
        orphans, _ = schema.validate_table_drop_list([table_name], list(table_drops.keys()))
        if orphans:
            table_drops.pop(table_name)
            logger.warning(
                f"Removing table '{table_name}' from the dlt schema would leave orphaned"
                f" table(s): {'.'.join(repr(t) for t in orphans)}. Drop these"
                " child tables in the destination and sync the dlt schema again."
            )

    # 3. If it's not a dry run, we actually drop from the dlt schema
    if not dry_run:
        schema.drop_tables(list(table_drops.keys()))
        for table_name, partial_table in column_drops.items():
            col_schemas = partial_table["columns"]
            col_names = [col for col in col_schemas]
            schema.drop_columns(table_name, col_names)

    return {**table_drops, **column_drops}
