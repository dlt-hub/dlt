import re
from typing import Any, List, Optional, Tuple

from dlt.common import logger
from dlt.common.schema import Schema
from dlt.common.schema.exceptions import SchemaCorruptedException
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_first_column_name_with_prop,
    has_column_with_prop,
)
from dlt.extract import DltResource, resource as make_resource

RE_DATA_TYPE = re.compile(r"([A-Z]+)\((\d+)(?:,\s?(\d+))?\)")


def ensure_resource(data: Any) -> DltResource:
    """Wraps `data` in a DltResource if it's not a DltResource already."""
    resource: DltResource
    if not isinstance(data, DltResource):
        resource_name: str = None
        if not hasattr(data, "__name__"):
            resource_name = "content"
        resource = make_resource(data, name=resource_name)
    else:
        resource = data
    return resource


def info_schema_null_to_bool(v: str) -> bool:
    """Converts INFORMATION SCHEMA truth values to Python bool"""
    if v == "NO":
        return False
    elif v == "YES":
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


def verify_sql_job_client_schema(schema: Schema, warnings: bool = True) -> List[Exception]:
    log = logger.warning if warnings else logger.info
    # collect all exceptions to show all problems in the schema
    exception_log: List[Exception] = []

    # verifies schema settings specific to sql job client
    for table in schema.data_tables():
        table_name = table["name"]
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
