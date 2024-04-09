import re
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

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
    "YYYY",
    "Y",
    "MMMM",  # January, February, March
    "MMM",  # Jan, Feb, Mar
    "MM",  # 01, 02, 03
    "M",  # 1, 2, 3
    "DD",  # 01, 02
    "D",  # 1, 2
    "HH",  # 00, 01, 02
    "H",  # 0, 1, 2
    "mm",
    "m",
    "dddd",  # Monday, Tuesday, Wednesday
    "ddd",  # Mon, Tue, Wed
    "dd",  # Mo, Tu, We
    "d",  # 0-6
    "Q",  # quarter 1, 2, 3, 4
    "timestamp",
}

SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS = ("schema_name",)


def get_placeholders(layout: str) -> List[str]:
    return re.findall(r"\{(.*?)\}", layout)


def prepare_datetime_params(
    current_datetime: Optional[pendulum.DateTime] = None,
    load_package_timestamp: Optional[str] = None,
) -> Dict[str, str]:
    # For formatting options please see
    # https://github.com/sdispater/pendulum/blob/master/docs/docs/string_formatting.md
    # Format curr_date datetime according to given format
    params: Dict[str, str] = {}
    now: pendulum.DateTime = current_datetime or pendulum.now()
    # we need to preserve load package timestamp if it is given
    # ideally it should always be given if not we take current datetime
    if load_package_timestamp:
        timestamp = pendulum.parse(load_package_timestamp)
    else:
        timestamp = now

    # Timestamp placeholder
    params["timestamp"] = str(int(timestamp.timestamp()))  # type: ignore[union-attr]

    # Take date from timestamp as curr_date
    params["curr_date"] = str(now.date())

    params["YYYY"] = now.format("Y")
    params["Y"] = now.format("Y")
    params["Q"] = "Q" + now.format("Q")

    # month, day, hour and minute padded with 0
    params["MMMM"] = now.format("MMMM")
    params["MMM"] = now.format("MMM")
    params["MM"] = now.format("MM")
    params["M"] = now.format("M")

    # Day of Month
    params["DD"] = now.format("DD")  # 01, 02, 03
    params["D"] = now.format("D")  # 1, 2, 3

    # Hour in 24h format
    params["HH"] = now.format("HH")
    params["H"] = now.format("H")

    # Minutes
    params["mm"] = now.format("mm")
    params["m"] = now.format("m")

    # Day of week
    params["dddd"] = now.format("dddd").lower()
    params["ddd"] = now.format("ddd").lower()
    params["dd"] = now.format("dd").lower()
    params["d"] = now.format("d").lower()

    return params


def prepare_params(
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

    if schema_name:
        params["schema_name"] = schema_name

    if load_id:
        params["load_id"] = load_id

    # Resolve extra placeholders
    if extra_placeholders:
        for key, value in extra_placeholders.items():
            if callable(value):
                try:
                    params[key] = value(
                        schema_name,
                        table_name,
                        load_id,
                        file_id,
                        ext,
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
    extra_placeholders: Optional[Dict[str, Any]] = None,
    allowed_placeholders: Optional[Set[str]] = SUPPORTED_PLACEHOLDERS,
) -> Tuple[List[str], List[str]]:
    """Returns a tuple with all valid placeholders and the list of layout placeholders

    Raises: InvalidFilesystemLayout

    Returns: a pair of lists of valid placeholders and layout placeholders
    """
    placeholders = get_placeholders(layout)
    # Build out the list of placeholder names
    # which we will use to validate placeholders
    # in a given config.layout template
    all_placeholders = allowed_placeholders.copy()
    if extra_placeholders:
        for placeholder, _ in extra_placeholders.items():
            all_placeholders.add(placeholder)

    # now collect all unknown placeholders from config.layout template
    invalid_placeholders = [p for p in placeholders if p not in all_placeholders]
    if invalid_placeholders:
        raise InvalidFilesystemLayout(invalid_placeholders)

    return list(all_placeholders), placeholders


def create_path(
    layout: str,
    file_name: str,
    schema_name: str,
    load_id: str,
    load_package_timestamp: Optional[str] = None,
    current_datetime: Optional[TCurrentDateTime] = None,
    extra_placeholders: Optional[Dict[str, Any]] = None,
) -> str:
    """create a filepath from the layout and our default params"""
    if callable(current_datetime):
        current_datetime = current_datetime()

    if current_datetime and not isinstance(current_datetime, pendulum.DateTime):
        raise RuntimeError("current_datetime is not an instance instance of pendulum.DateTime")

    job_info = ParsedLoadJobFileName.parse(file_name)
    params = prepare_params(
        extra_placeholders=extra_placeholders,
        job_info=job_info,
        schema_name=schema_name,
        load_id=load_id,
    )

    datetime_params = prepare_datetime_params(current_datetime, load_package_timestamp)
    params.update(datetime_params)

    placeholders, layout_placeholders = check_layout(layout, params)

    # Find and show unused placeholders
    unused_placeholders: List[str] = []
    if extra_placeholders:
        unused_placeholders = [p for p in extra_placeholders.keys() if p not in layout_placeholders]
        if unused_placeholders:
            fmt.secho(f"Found unused layout placeholders: {', '.join(unused_placeholders)}")

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
    placeholders = get_placeholders(layout)
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
