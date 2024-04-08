import re
from types import TracebackType
from typing import Any, Dict, List, Optional, Sequence, Set, Type

import pendulum

from dlt.cli import echo as fmt
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.destinations.exceptions import CantExtractTablePrefix, InvalidFilesystemLayout
from typing_extensions import Self

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


def prepare_datetime_params(
    moment: pendulum.DateTime,
    datetime_format: Optional[str] = None,
    load_package_timestamp: Optional[str] = None,
) -> Dict[str, str]:
    # For formatting options please see
    # https://github.com/sdispater/pendulum/blob/master/docs/docs/string_formatting.md
    # Format curr_date datetime according to given format
    params: Dict[str, str] = {}
    if datetime_format:
        params["curr_date"] = moment.format(datetime_format)
    else:
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

    if load_package_timestamp:
        timestamp = int(pendulum.parse(load_package_timestamp).timestamp())  # type: ignore[union-attr]
        params["timestamp"] = str(timestamp)

    return params


class LayoutParams:
    def __init__(
        self,
        current_datetime: TCurrentDateTime = None,
        datetime_format: Optional[str] = None,
        extra_placeholders: Optional[Dict[str, Any]] = None,
        job_info: Optional[ParsedLoadJobFileName] = None,
        schema_name: Optional[str] = None,
        load_id: Optional[str] = None,
        load_package_timestamp: Optional[str] = None,
    ) -> None:
        self.current_datetime = current_datetime
        self.datetime_format = datetime_format
        self.extra_placeholders = extra_placeholders
        self.job_info = job_info
        self.load_id = load_id
        self.schema_name = schema_name
        self.load_package_timestamp = load_package_timestamp
        self._params: Dict[str, Any] = {"schema_name": schema_name}

        self.table_name = None
        self.file_id = None
        self.ext = None
        if job_info:
            self.table_name = job_info.table_name
            self.file_id = job_info.file_id
            self.ext = job_info.file_format
            self._params.update(
                {
                    "table_name": self.table_name,
                    "file_id": self.file_id,
                    "ext": self.ext,
                }
            )

        if self.load_id:
            self._params["load_id"] = self.load_id

    @property
    def params(self) -> Optional[Dict[str, Any]]:
        """Process extra params for layout
        If any value is a callable then we call it with the following arguments
            * schema name,
            * table name,
            * load id,
            * file id,
            * current datetime
        """
        if not self.current_datetime:
            self.current_datetime = pendulum.now()

        # If current_datetime is callable
        # Then call it and check it's instance
        # If the result id DateTime
        # Then take it
        # Else exit.
        if callable(self.current_datetime):
            result = self.current_datetime()
            if isinstance(result, pendulum.DateTime):
                self.current_datetime = result
            else:
                raise RuntimeError(
                    "current_datetime was passed as callable but "
                    "didn't return any instance of pendulum.DateTime"
                )

        self._process_extra_placeholders()
        date_placeholders = prepare_datetime_params(
            self.current_datetime,
            self.datetime_format,
            self.load_package_timestamp,
        )
        self._params.update(date_placeholders)
        return self._params

    def _process_extra_placeholders(self) -> None:
        # For each callable extra parameter
        # otherwise take it's value
        if not self.extra_placeholders:
            return

        for key, value in self.extra_placeholders.items():
            if callable(value):
                try:
                    self._params[key] = value(
                        self.schema_name,
                        self.table_name,
                        self.load_id,
                        self.file_id,
                        self.ext,
                        self.current_datetime,
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
                self._params[key] = value


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
    load_package_timestamp: Optional[str] = None,
    current_datetime: TCurrentDateTime = None,
    datetime_format: Optional[str] = None,
    extra_placeholders: Optional[Dict[str, Any]] = None,
) -> str:
    """create a filepath from the layout and our default params"""
    job_info = ParsedLoadJobFileName.parse(file_name)
    extras = LayoutParams(
        current_datetime=current_datetime,
        datetime_format=datetime_format,
        extra_placeholders=extra_placeholders,
        job_info=job_info,
        schema_name=schema_name,
        load_id=load_id,
        load_package_timestamp=load_package_timestamp,
    )

    placeholders = check_layout(layout, extras.params)
    path = layout.format(**extras.params)

    # if extension is not defined, we append it at the end
    if "ext" not in placeholders:
        path += f".{extras.ext}"

    return path


def get_table_prefix_layout(
    layout: str,
    supported_prefix_placeholders: Sequence[str] = SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS,
    current_datetime: TCurrentDateTime = None,
    datetime_format: Optional[str] = None,
    extra_placeholders: Optional[Dict[str, Any]] = None,
) -> str:
    """get layout fragment that defines positions of the table, cutting other placeholders
    allowed `supported_prefix_placeholders` that may appear before table.
    """
    extras = LayoutParams(
        current_datetime=current_datetime,
        datetime_format=datetime_format,
        extra_placeholders=extra_placeholders,
    )

    placeholders = check_layout(layout, extras.params)
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
