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
    moment: pendulum.DateTime, datetime_format: Optional[str] = None
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
    params["timestamp"] = str(int(moment.timestamp()))

    return params


class ExtraParams:
    def __init__(
        self,
        current_datetime: TCurrentDateTime = None,
        datetime_format: Optional[str] = None,
        extra_placeholders: Optional[Dict[str, Any]] = None,
        job_info: Optional[ParsedLoadJobFileName] = None,
        schema_name: Optional[str] = None,
        load_id: Optional[str] = None,
    ) -> None:
        self.current_datetime = current_datetime
        self.datetime_format = datetime_format
        self.extra_placeholders = extra_placeholders
        self.job_info = job_info
        self.load_id = load_id
        self.schema_name = schema_name
        self._params: Dict[str, Any] = {"schema_name": schema_name}

        self.table_name = None
        self.file_id = None
        self.file_format = None
        if job_info:
            self.table_name = job_info.table_name
            self.file_id = job_info.file_id
            self.file_format = job_info.file_format
            self._params.update(
                {
                    "table_name": self.table_name,
                    "file_id": self.file_id,
                    "ext": self.file_format,
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
        date_placeholders = prepare_datetime_params(self.current_datetime, self.datetime_format)
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
                        self.file_format,
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


class LayoutHelper:
    def __init__(
        self,
        layout: str,
        params: Dict[str, str],
        allowed_placeholders: Optional[Set[str]] = SUPPORTED_PLACEHOLDERS,
    ) -> None:
        self.params = params
        self.allowed_placeholders = allowed_placeholders.copy()
        self.layout_placeholders = get_placeholders(layout)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        pass

    @property
    def placeholders(self) -> List[str]:
        self.check_layout()
        return list(self.allowed_placeholders)

    def check_layout(self) -> None:
        # Build out the list of placeholder names
        # which we will use to validate placeholders
        # in a given config.layout template
        if self.params:
            for placeholder, _ in self.params.items():
                self.allowed_placeholders.add(placeholder)

        # now collect all unknown placeholders from config.layout template
        invalid_placeholders = [
            p for p in self.layout_placeholders if p not in self.allowed_placeholders
        ]
        if invalid_placeholders:
            raise InvalidFilesystemLayout(invalid_placeholders)


def create_path(
    layout: str,
    file_name: str,
    schema_name: str,
    load_id: str,
    current_datetime: TCurrentDateTime = None,
    datetime_format: Optional[str] = None,
    extra_placeholders: Optional[Dict[str, Any]] = None,
) -> str:
    """create a filepath from the layout and our default params"""
    job_info = ParsedLoadJobFileName.parse(file_name)
    extras = ExtraParams(
        current_datetime=current_datetime,
        datetime_format=datetime_format,
        extra_placeholders=extra_placeholders,
        job_info=job_info,
        schema_name=schema_name,
        load_id=load_id,
    )

    with LayoutHelper(layout, extras.params) as layout_helper:
        placeholders = layout_helper.placeholders
        path = layout.format(**extras.params)

        # if extension is not defined, we append it at the end
        if "ext" not in placeholders:
            path += f".{job_info.file_format}"

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
    extras = ExtraParams(
        current_datetime=current_datetime,
        datetime_format=datetime_format,
        extra_placeholders=extra_placeholders,
    )
    with LayoutHelper(layout, extras.params) as layout_helper:
        # fail if table name is not defined
        if "table_name" not in layout_helper.layout_placeholders:
            raise CantExtractTablePrefix(layout, "{table_name} placeholder not found. ")

    table_name_index = layout_helper.layout_placeholders.index("table_name")

    # fail if any other prefix is defined before table_name
    if [
        p
        for p in layout_helper.layout_placeholders[:table_name_index]
        if p not in supported_prefix_placeholders
    ]:
        if len(supported_prefix_placeholders) == 0:
            details = (
                "No other placeholders are allowed before {table_name} but you have %s present. "
                % layout_helper.layout_placeholders[:table_name_index]
            )
        else:
            details = "Only %s are allowed before {table_name} but you have %s present. " % (
                supported_prefix_placeholders,
                layout_helper.layout_placeholders[:table_name_index],
            )
        raise CantExtractTablePrefix(layout, details)

    # we include the char after the table_name here, this should be a separator not a new placeholder
    # this is to prevent selecting tables that have the same starting name -> {table_name}/
    prefix = layout[: layout.index("{table_name}") + 13]
    if prefix[-1] == "{":
        raise CantExtractTablePrefix(layout, "A separator is required after a {table_name}. ")

    return prefix
