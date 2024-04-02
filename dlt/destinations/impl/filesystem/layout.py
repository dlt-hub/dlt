import re
from types import TracebackType
from typing import Any, Dict, List, Optional, Self, Sequence, Set, Type

import pendulum

from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.destinations.exceptions import CantExtractTablePrefix, InvalidFilesystemLayout
from dlt.destinations.impl.filesystem.configuration import (
    FilesystemDestinationClientConfiguration,
)

# TODO: ensure layout only has supported placeholders
SUPPORTED_PLACEHOLDERS = {
    "schema_name",
    "table_name",
    "load_id",
    "file_id",
    "ext",
    "timestamp",
    "curr_date",
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "dow",
}

SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS = ("schema_name",)


class extra_params:
    def __init__(
        self,
        config: FilesystemDestinationClientConfiguration,
        job_info: Optional[ParsedLoadJobFileName] = None,
        schema_name: Optional[str] = None,
        load_id: Optional[str] = None,
    ) -> None:
        self.config = config
        self.job_info = job_info
        self.load_id = load_id
        self.schema_name = schema_name
        self._params = {}

        self.table_name = None
        self.file_id = None
        self.file_format = None
        if job_info:
            self.table_name = job_info.table_name
            self.file_id = job_info.file_id
            self.file_format = job_info.file_format

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
    def params(self) -> Optional[Dict[str, Any]]:
        """Process extra params for layout
        If any value is a callable then we call it with the following arguments

            * schema name,
            * table name,
            * load id,
            * file id,
            * current datetime
        """
        if self.load_id:
            self._params["load_id"] = self.load_id

        if self.job_info:
            self._params.update(
                {
                    "table_name": self.table_name,
                    "file_id": self.file_id,
                    "ext": self.file_format,
                }
            )

        now = self.config.current_datetime or pendulum.now()
        for key, value in self.config.extra_params.items():
            if callable(value):
                self._params[key] = value(
                    self.schema_name,
                    self.table_name,
                    self.load_id,
                    self.file_id,
                    now,
                )
            else:
                self._params[key] = value

        # For formatting options please see
        # https://github.com/sdispater/pendulum/blob/master/docs/docs/string_formatting.md
        self._params["year"] = now.year
        # month, day, hour and minute padded with 0
        self._params["month"] = now.format("MM")
        # Days in format Mon, Tue, Wed
        self._params["day"] = now.format("DD")
        # Hour in 24h format
        self._params["hour"] = now.format("HH")
        self._params["minute"] = now.format("mm")
        # Day of week
        self._params["dow"] = now.format("ddd").lower()
        self._params["timestamp"] = int(now.timestamp())

        # Format curr_date datetime according to given format
        if self.config.datetime_format:
            self._params["curr_date"] = now.format(self.config.datetime_format)
        else:
            self._params["curr_date"] = str(now.date())

        return self._params


class layout_helper:
    def __init__(
        self,
        path_layout: str,
        params: Dict[str, str],
        allowed_placeholders: Optional[Set[str]] = SUPPORTED_PLACEHOLDERS,
    ) -> None:
        self.params = params
        self.allowed_placeholders = allowed_placeholders.copy()
        self.layout_placeholders = re.findall(r"\{(.*?)\}", path_layout)

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


def make_filename(
    config: FilesystemDestinationClientConfiguration,
    file_name: str,
    schema_name: str,
    load_id: str,
) -> str:
    job_info = ParsedLoadJobFileName.parse(file_name)
    with extra_params(
        config,
        job_info,
        schema_name,
        load_id,
    ) as extras, layout_helper(
        config.layout,
        extras.params,
    ) as layout:
        placeholders = layout.placeholders
        path = config.layout.format(**extras.params)

        # if extension is not defined, we append it at the end
        if "ext" not in placeholders:
            path += f".{extras.file_format}"

        return path


def get_table_prefix_layout(
    config: FilesystemDestinationClientConfiguration,
    supported_prefix_placeholders: Sequence[str] = SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS,
) -> str:
    """get layout fragment that defines positions of the table, cutting other placeholders

    allowed `supported_prefix_placeholders` that may appear before table.
    """
    with extra_params(config) as extras, layout_helper(
        config.layout,
        extras.params,
    ) as layout:
        # fail if table name is not defined
        if "table_name" not in layout.placeholders:
            raise CantExtractTablePrefix(layout, "{table_name} placeholder not found. ")

    table_name_index = layout.layout_placeholders.index("table_name")

    # fail if any other prefix is defined before table_name
    if [
        p
        for p in layout.layout_placeholders[:table_name_index]
        if p not in supported_prefix_placeholders
    ]:
        if len(supported_prefix_placeholders) == 0:
            details = (
                "No other placeholders are allowed before {table_name} but you have %s present. "
                % layout.layout_placeholders[:table_name_index]
            )
        else:
            details = "Only %s are allowed before {table_name} but you have %s present. " % (
                supported_prefix_placeholders,
                layout.layout_placeholders[:table_name_index],
            )
        raise CantExtractTablePrefix(config.layout, details)

    # we include the char after the table_name here, this should be a separator not a new placeholder
    # this is to prevent selecting tables that have the same starting name -> {table_name}/
    prefix = config.layout[: config.layout.index("{table_name}") + 13]

    if prefix[-1] == "{":
        raise CantExtractTablePrefix(
            config.layout, "A separator is required after a {table_name}. "
        )

    return prefix
