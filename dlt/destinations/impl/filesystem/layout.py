import re
from types import TracebackType
from typing import Any, Dict, List, Optional, Self, Set, Type

import pendulum

from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.destinations.exceptions import InvalidFilesystemLayout
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
}

SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS = ("schema_name",)


class extra_params:
    def __init__(
        self,
        config: FilesystemDestinationClientConfiguration,
        job_info: ParsedLoadJobFileName,
        schema_name: str,
        load_id: str,
    ) -> None:
        self.config = config
        self.job_info = job_info
        self.load_id = load_id
        self.schema_name = schema_name
        self._params = {}

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
        if self._params:
            return self._params

        self._params = {
            "load_id": self.load_id,
            "file_id": self.job_info.file_id,
            "ext": self.job_info.file_format,
            "table_name": self.job_info.table_name,
        }
        now = self.config.current_datetime or pendulum.now()
        for key, value in self.config.extra_params.items():
            if callable(value):
                self._params[key] = value(
                    self.schema_name,
                    self.job_info.table_name,
                    self.load_id,
                    self.job_info.file_id,
                    now,
                )
            else:
                self._params[key] = value

        self._params["year"] = now.year
        self._params["month"] = now.month
        self._params["day"] = now.day
        self._params["hour"] = now.hour
        self._params["minute"] = now.minute
        self._params["timestamp"] = int(now.timestamp())

        # Format curr_date datetime according to given format
        if self.config.datetime_format:
            self._params["curr_date"] = now.format(self.config.datetime_format)
        else:
            self._params["curr_date"] = now.isoformat()

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
            path += f".{extras.params['ext']}"

        if config.suffix:
            suffix = config.suffix
            if callable(suffix):
                suffix = suffix(extras.params)
            path += suffix
        # import ipdb;ipdb.set_trace()

        return path
