import re
import os
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from dlt.common import logger
from dlt.common.pendulum import pendulum
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.time import (
    ensure_pendulum_datetime,
    datetime_to_timestamp,
    datetime_to_timestamp_ms,
)
from dlt.destinations.exceptions import (
    CantExtractTablePrefix,
    InvalidFilesystemLayout,
    InvalidPlaceholderCallback,
)
from dlt.destinations.impl.filesystem.typing import TCurrentDateTime


# For formatting options please see
# https://github.com/sdispater/pendulum/blob/master/docs/docs/string_formatting.md
DATETIME_PLACEHOLDERS = {
    # Years
    "YYYY",  # 2024, 2025, 2026
    "YY",  # 00, 01, 02 ... 12, 13
    "Y",  # 2024, 2025, 2026
    # Months
    "MMMM",  # January, February, March
    "MMM",  # Jan, Feb, Mar
    "MM",  # 01-12
    "M",  # 1-12
    # Days
    "DD",  # 01-31
    "D",  # 1-31
    # Hours
    "HH",  # 00-23
    "H",  # 0-23
    # Minutes
    "mm",  # 00-59
    "m",  # 0-59
    # Days of week
    "dddd",  # Monday, Tuesday, Wednesday
    "ddd",  # Mon, Tue, Wed
    "dd",  # Mo, Tu, We
    "d",  # 0-6
    # Seconds
    "ss",  # 01-59
    "s",  # 0-59
    # Fractional seconds
    "SSSS",  # 000[0..] 001[0..] ... 998[0..] 999[0..]
    "SSS",  # 000 001 ... 998 999
    "SS",  # 00, 01, 02 ... 98, 99
    "S",  # 0 1 ... 8 9
    # Quarters of the year
    "Q",  # 1, 2, 3, 4
}

STANDARD_PLACEHOLDERS = DATETIME_PLACEHOLDERS.union(
    {
        "schema_name",
        "table_name",
        "load_id",
        "file_id",
        "ext",
        "curr_date",
        "timestamp",
        "timestamp",
        "timestamp_ms",
        "load_package_timestamp",
        "load_package_timestamp_ms",
    }
)


SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS = ("schema_name",)


def normalize_path_sep(pathlib: Any, path: str) -> str:
    """Normalizes path in `path` separator to one used by `pathlib`"""
    if pathlib.sep == "/":
        return path.replace("\\", "/")
    if pathlib.sep == "\\":
        return path.replace("/", "\\")
    return path


def get_placeholders(layout: str) -> List[str]:
    return re.findall(r"\{(.*?)\}", layout)


def get_unused_placeholders(
    layout_placeholders: Sequence[str],
    extra_placeholders: Sequence[str],
) -> Sequence[str]:
    unused_placeholders = [p for p in extra_placeholders if p not in layout_placeholders]
    return unused_placeholders


def prepare_datetime_params(
    current_datetime: Optional[pendulum.DateTime] = None,
    load_package_timestamp: Optional[pendulum.DateTime] = None,
) -> Dict[str, str]:
    params: Dict[str, str] = {}
    current_timestamp: pendulum.DateTime = None
    if load_package_timestamp:
        current_timestamp = ensure_pendulum_datetime(load_package_timestamp)
        params["load_package_timestamp"] = str(datetime_to_timestamp(current_timestamp))
        params["load_package_timestamp_ms"] = str(datetime_to_timestamp_ms(current_timestamp))

    if not current_datetime:
        if current_timestamp:
            logger.info("current_datetime is not set, using timestamp from load package")
            current_datetime = current_timestamp
        else:
            logger.info("current_datetime is not set, using pendulum.now()")
            current_datetime = pendulum.now()

    params["timestamp"] = str(datetime_to_timestamp(current_datetime))
    params["timestamp_ms"] = str(datetime_to_timestamp_ms(current_datetime))
    params["curr_date"] = str(current_datetime.date())

    for format_string in DATETIME_PLACEHOLDERS:
        params[format_string] = current_datetime.format(format_string).lower()

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
        ext = job_info.full_extension()
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
                except TypeError as exc:
                    raise InvalidPlaceholderCallback(key) from exc
            else:
                params[key] = value

    return params


def check_layout(
    layout: str,
    extra_placeholders: Optional[Dict[str, Any]] = None,
    standard_placeholders: Optional[Set[str]] = STANDARD_PLACEHOLDERS,
) -> Tuple[List[str], List[str]]:
    """Returns a tuple with all valid placeholders and the list of layout placeholders

    Raises: InvalidFilesystemLayout

    Returns: a pair of lists of valid placeholders and layout placeholders
    """
    placeholders = get_placeholders(layout)
    # Build out the list of placeholder names
    # which we will use to validate placeholders
    # in a given config.layout template
    all_placeholders = standard_placeholders.copy()
    if extra_placeholders:
        for placeholder, _ in extra_placeholders.items():
            all_placeholders.add(placeholder)

    # now collect all unknown placeholders from config.layout template
    invalid_placeholders = [p for p in placeholders if p not in all_placeholders]
    extra_placeholder_keys = list((extra_placeholders or {}).keys())
    unused_placeholders = get_unused_placeholders(placeholders, extra_placeholder_keys)
    if invalid_placeholders:
        raise InvalidFilesystemLayout(
            layout,
            all_placeholders,  # type: ignore[arg-type]
            extra_placeholder_keys,
            invalid_placeholders,
            unused_placeholders,
        )

    return list(all_placeholders), placeholders


def create_path(
    layout: str,
    file_name: str,
    schema_name: str,
    load_id: str,
    load_package_timestamp: Optional[pendulum.DateTime] = None,
    current_datetime: Optional[TCurrentDateTime] = None,
    extra_placeholders: Optional[Dict[str, Any]] = None,
) -> str:
    """create a filepath from the layout and our default params"""
    if callable(current_datetime):
        current_datetime = current_datetime()
        if not isinstance(current_datetime, pendulum.DateTime):
            raise RuntimeError("`current_datetime` is not an instance of `pendulum.DateTime`")

    job_info = ParsedLoadJobFileName.parse(file_name)
    params = prepare_params(
        extra_placeholders=extra_placeholders,
        job_info=job_info,
        schema_name=schema_name,
        load_id=load_id,
    )

    datetime_params = prepare_datetime_params(current_datetime, load_package_timestamp)
    params.update(datetime_params)

    placeholders, _ = check_layout(layout, params)
    path = layout.format(**params)

    # if extension is not defined, we append it at the end
    if "ext" not in placeholders:
        path = job_info.full_extension()

    return path


def get_table_prefix_layout(
    layout: str,
    supported_prefix_placeholders: Sequence[str] = SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS,
    table_needs_own_folder: bool = False,
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
    if prefix[-1] != "/" and table_needs_own_folder:
        raise CantExtractTablePrefix(
            layout, "Table requires it's own folder, please add a '/' after your {table_name}. "
        )

    return prefix


def get_file_format_and_compression(file_path: str) -> Tuple[str, bool]:
    """Return file format and whether an explicit .gz compression extension is present.
    Primarily used in cases that ParsedLoadJobFileName.parse() can't handle, that is,
    staging storage files that have a configurabe layout."""
    root, ext = os.path.splitext(file_path)

    file_format: str = None
    compression_ext: bool = False

    if ext == ".gz":
        compression_ext = True
        _, ext = os.path.splitext(root)
        file_format = ext[1:]  # remove the dot
    else:
        file_format = ext[1:]  # remove the dot

    return file_format, compression_ext
