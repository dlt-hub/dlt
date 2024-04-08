import re
from typing import Any, Dict, List, Optional, Sequence, Set

import pendulum

from dlt.cli import echo as fmt
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.destinations.exceptions import CantExtractTablePrefix, InvalidFilesystemLayout

from dlt.destinations.impl.filesystem.typing import TCurrentDateTime


SUPPORTED_PLACEHOLDERS = {
    "schema_name",
    "table_name",
    "load_id",
    "file_id",
    "ext",
    "curr_date",
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "dow",
    "timestamp",
}

SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS = ("schema_name",)


def get_placeholders(layout: str) -> List[str]:
    return re.findall(r"\{(.*?)\}", layout)


def prepare_datetime_params(load_package_timestamp: str) -> Dict[str, str]:
    # For formatting options please see
    # https://github.com/sdispater/pendulum/blob/master/docs/docs/string_formatting.md
    # Format curr_date datetime according to given format
    moment = pendulum.parse(load_package_timestamp)
    params: Dict[str, str] = {}

    # Timestamp placeholder
    params["timestamp"] = str(int(moment.timestamp()))

    # Take date from timestamp as curr_date
    params["curr_date"] = str(moment.date())

    params["year"] = str(moment.year)
    # month, day, hour and minute padded with 0
    params["month"] = moment.format("MM")
    # Days in format Mon, Tue, Wed
    params["day"] = moment.format("DD")
    # Hour in 24h format
    params["hour"] = moment.format("HH")
    params["minute"] = moment.format("mm")
    # Day of week
    params["dow"] = moment.format("ddd").lower()

    return params


def prepare_params(
    current_datetime: TCurrentDateTime = None,
    extra_placeholders: Optional[Dict[str, Any]] = None,
    job_info: Optional[ParsedLoadJobFileName] = None,
    schema_name: Optional[str] = None,
    load_id: Optional[str] = None,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    table_name = None
    file_id = None
    ext = None
    if job_info:
        table_name = job_info.table_name
        file_id = job_info.file_id
        ext = job_info.file_format
        params.update(
            {
                "table_name": table_name,
                "file_id": file_id,
                "ext": ext,
            }
        )

    if load_id:
        params["load_id"] = load_id

    # Resolve extra placeholders
    for key, value in extra_placeholders.items():
        if callable(value):
            try:
                params[key] = value(
                    schema_name,
                    table_name,
                    load_id,
                    file_id,
                    ext,
                    current_datetime,
                )
            except TypeError:
                fmt.secho(
                    f"Extra placeholder {key} is callableCallable placeholder should accept"
                    " parameters below`schema name`, `table name`, `load_id`, `file_id`,"
                    " `extension` and `current_datetime`",
                    fg="red",
                )
                raise
        else:
            params[key] = value

    return params


def check_layout(
    layout: str,
    params: Optional[Dict[str, Any]] = None,
    allowed_placeholders: Optional[Set[str]] = SUPPORTED_PLACEHOLDERS,
) -> List[str]:
    placeholders = get_placeholders(layout)
    # Build out the list of placeholder names
    # which we will use to validate placeholders
    # in a given config.layout template
    all_placeholders = allowed_placeholders.copy()
    if params:
        for placeholder, _ in params.items():
            all_placeholders.add(placeholder)

    # now collect all unknown placeholders from config.layout template
    invalid_placeholders = [p for p in placeholders if p not in allowed_placeholders]
    if invalid_placeholders:
        raise InvalidFilesystemLayout(invalid_placeholders)
    return list(all_placeholders)


def create_path(
    layout: str,
    file_name: str,
    schema_name: str,
    load_id: str,
    load_package_timestamp: str,
    current_datetime: TCurrentDateTime = None,
    datetime_format: Optional[str] = None,
    extra_placeholders: Optional[Dict[str, Any]] = None,
) -> str:
    """create a filepath from the layout and our default params"""
    job_info = ParsedLoadJobFileName.parse(file_name)
    params = prepare_params(
        current_datetime=current_datetime,
        datetime_format=datetime_format,
        extra_placeholders=extra_placeholders,
        job_info=job_info,
        schema_name=schema_name,
        load_id=load_id,
    )

    datetime_params = prepare_datetime_params(load_package_timestamp)
    params.update(datetime_params)
    placeholders = check_layout(layout, params)
    path = layout.format(**params)

    # if extension is not defined, we append it at the end
    if "ext" not in placeholders:
        path += f".{job_info.file_format}"

    return path


def get_table_prefix_layout(
    layout: str,
    supported_prefix_placeholders: Sequence[str] = SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS,
) -> str:
    """get layout fragment that defines positions of the table, cutting other placeholders
    allowed `supported_prefix_placeholders` that may appear before table.
    """
    placeholders = check_layout(layout)
    # fail if table name is not defined
    if "table_name" not in placeholders:
        raise CantExtractTablePrefix(layout, "{table_name} placeholder not found. ")

    table_name_index = placeholders.index("table_name")

    # fail if any other prefix is defined before table_name
    if [p for p in placeholders[:table_name_index] if p not in supported_prefix_placeholders]:
        if len(supported_prefix_placeholders) == 0:
            details = (
                "No other placeholders are allowed before {table_name} but you have %s present. "
                % placeholders[:table_name_index]
            )
        else:
            details = "Only %s are allowed before {table_name} but you have %s present. " % (
                supported_prefix_placeholders,
                placeholders[:table_name_index],
            )
        raise CantExtractTablePrefix(layout, details)

    # we include the char after the table_name here, this should be a separator not a new placeholder
    # this is to prevent selecting tables that have the same starting name -> {table_name}/
    prefix = layout[: layout.index("{table_name}") + 13]
    if prefix[-1] == "{":
        raise CantExtractTablePrefix(layout, "A separator is required after a {table_name}. ")

    return prefix
