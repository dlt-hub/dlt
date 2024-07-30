import re
import inspect

from typing import Any, List, Optional, Tuple

from dlt.common import logger
from dlt.common.schema import Schema
from dlt.common.schema.exceptions import SchemaCorruptedException
from dlt.common.schema.typing import MERGE_STRATEGIES, TTableSchema
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_first_column_name_with_prop,
    has_column_with_prop,
    pipeline_state_table,
)
from typing import Any, cast, Tuple, Dict, Type

from dlt.destinations.exceptions import DatabaseTransientException
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
                "You are trying to use an adapter on a DltSource with multiple resources. You can"
                " only use adapters on pure data, direclty on a DltResouce or a DltSource"
                " containing a single DltResource."
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


def verify_sql_job_client_schema(schema: Schema, warnings: bool = True) -> List[Exception]:
    log = logger.warning if warnings else logger.info
    # collect all exceptions to show all problems in the schema
    exception_log: List[Exception] = []

    # verifies schema settings specific to sql job client
    for table in schema.data_tables():
        table_name = table["name"]
        if table.get("write_disposition") == "merge":
            if "x-merge-strategy" in table and table["x-merge-strategy"] not in MERGE_STRATEGIES:  # type: ignore[typeddict-item]
                exception_log.append(
                    SchemaCorruptedException(
                        schema.name,
                        f'"{table["x-merge-strategy"]}" is not a valid merge strategy. '  # type: ignore[typeddict-item]
                        f"""Allowed values: {', '.join(['"' + s + '"' for s in MERGE_STRATEGIES])}.""",
                    )
                )
            if table.get("x-merge-strategy") == "delete-insert":
                if not has_column_with_prop(table, "primary_key") and not has_column_with_prop(
                    table, "merge_key"
                ):
                    log(
                        f"Table {table_name} has `write_disposition` set to `merge`"
                        " and `merge_strategy` set to `delete-insert`, but no primary or"
                        " merge keys defined."
                        " dlt will fall back to `append` for this table."
                    )
            elif table.get("x-merge-strategy") == "upsert":
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
            if table.get("write_disposition") == "merge" and not has_column_with_prop(
                table, "primary_key"
            ):
                log(
                    f"""The "dedup_sort" column hint for column "{get_first_column_name_with_prop(table, 'dedup_sort')}" """
                    f'in table "{table_name}" with write disposition'
                    f' "{table.get("write_disposition")}"'
                    f' in schema "{schema.name}" will be ignored.'
                    ' The "dedup_sort" column hint is only applied when a'
                    " primary key has been specified."
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
