import logging
from datetime import datetime  # noqa: I251
import traceback
from logging import Logger
from typing import Any, List, Type

from dlt.common.json import json
from dlt.common.typing import DictStrAny, StrAny

EMPTY_VALUE = "-"
JSON_SERIALIZER = lambda log: json.dumps(log)
COMPONENT_ID = EMPTY_VALUE
COMPONENT_NAME = EMPTY_VALUE
COMPONENT_INSTANCE_INDEX = 0

# The list contains all the attributes listed in
# http://docs.python.org/library/logging.html#logrecord-attributes
RECORD_ATTR_SKIP_LIST = [
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "args",
    "funcName",
    "id",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msg",
    "msecs",
    "msecs",
    "message",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "thread",
    "threadName",
    "extra",
    # Also exclude legacy 'props'
    "props",
]

RECORD_ATTR_SKIP_LIST.append("stack_info")
EASY_TYPES = (str, bool, dict, float, int, list, type(None))

_default_formatter: Type[logging.Formatter] = None
_epoch = datetime(1970, 1, 1)


def config_root_logger() -> None:
    """
    You must call this if you are using root logger.
    Make all root logger' handlers produce JSON format
    & remove duplicate handlers for request instrumentation logging.
    Please made sure that you call this after you called "logging.basicConfig() or logging.getLogger()
    """
    global _default_formatter
    update_formatter_for_loggers([logging.root], _default_formatter)


def init(custom_formatter: Type[logging.Formatter] = None) -> None:
    """
    This is supposed to be called only one time.

    If **custom_formatter** is passed, it will (in non-web context) use this formatter over the default.
    """

    global _default_formatter

    if custom_formatter:
        if not issubclass(custom_formatter, logging.Formatter):
            raise ValueError(
                "custom_formatter is not subclass of logging.Formatter", custom_formatter
            )

    _default_formatter = custom_formatter if custom_formatter else JSONLogFormatter
    logging._defaultFormatter = _default_formatter()  # type: ignore

    # go to all the initialized logger and update it to use JSON formatter
    existing_loggers = list(map(logging.getLogger, logging.Logger.manager.loggerDict))
    update_formatter_for_loggers(existing_loggers, _default_formatter)


class BaseJSONFormatter(logging.Formatter):
    """
    Base class for JSON formatters
    """

    base_object_common: DictStrAny = {}

    def __init__(self, *args: Any, **kw: Any) -> None:
        super(BaseJSONFormatter, self).__init__(*args, **kw)
        if COMPONENT_ID and COMPONENT_ID != EMPTY_VALUE:
            self.base_object_common["component_id"] = COMPONENT_ID
        if COMPONENT_NAME and COMPONENT_NAME != EMPTY_VALUE:
            self.base_object_common["component_name"] = COMPONENT_NAME
        if COMPONENT_INSTANCE_INDEX and COMPONENT_INSTANCE_INDEX != EMPTY_VALUE:
            self.base_object_common["component_instance_idx"] = COMPONENT_INSTANCE_INDEX

    def format(self, record: logging.LogRecord) -> str:  # noqa
        log_object = self._format_log_object(record)
        return JSON_SERIALIZER(log_object)

    def _format_log_object(self, record: logging.LogRecord) -> DictStrAny:
        utcnow = datetime.utcnow()
        base_obj = {
            "written_at": iso_time_format(utcnow),
            "written_ts": epoch_nano_second(utcnow),
        }
        base_obj.update(self.base_object_common)
        # Add extra fields
        base_obj.update(self._get_extra_fields(record))
        return base_obj

    def _get_extra_fields(self, record: logging.LogRecord) -> StrAny:
        fields: DictStrAny = {}

        if record.args:
            fields["msg"] = record.msg

        for key, value in record.__dict__.items():
            if key not in RECORD_ATTR_SKIP_LIST:
                if isinstance(value, EASY_TYPES):
                    fields[key] = value
                else:
                    fields[key] = repr(value)

        # Always add 'props' to the root of the log, assumes props is a dict
        if hasattr(record, "props") and isinstance(record.props, dict):
            fields.update(record.props)

        return fields


def _sanitize_log_msg(record: logging.LogRecord) -> str:
    return record.getMessage().replace("\n", "_").replace("\r", "_").replace("\t", "_")


class JSONLogFormatter(BaseJSONFormatter):
    """
    Formatter for non-web application log
    """

    def get_exc_fields(self, record: logging.LogRecord) -> StrAny:
        if record.exc_info:
            exc_info = self.format_exception(record.exc_info)
        else:
            exc_info = record.exc_text
        return {
            "exc_info": exc_info,
            "filename": record.filename,
        }

    @classmethod
    def format_exception(cls, exc_info: Any) -> str:
        return "".join(traceback.format_exception(*exc_info)) if exc_info else ""

    def _format_log_object(self, record: logging.LogRecord) -> DictStrAny:
        json_log_object = super(JSONLogFormatter, self)._format_log_object(record)
        json_log_object.update(
            {
                "msg": _sanitize_log_msg(record),
                "type": "log",
                "logger": record.name,
                "thread": record.threadName,
                "level": record.levelname,
                "module": record.module,
                "line_no": record.lineno,
            }
        )

        if record.exc_info or record.exc_text:
            json_log_object.update(self.get_exc_fields(record))

        return json_log_object


def update_formatter_for_loggers(
    loggers_iter: List[Logger], formatter: Type[logging.Formatter]
) -> None:
    """
    :param formatter:
    :param loggers_iter:
    """
    for logger in loggers_iter:
        if not isinstance(logger, Logger):
            raise RuntimeError("%s is not a logging.Logger instance", logger)
        for handler in logger.handlers:
            if not isinstance(handler.formatter, formatter):
                handler.formatter = formatter()


def epoch_nano_second(datetime_: datetime) -> int:
    return int((datetime_ - _epoch).total_seconds()) * 1000000000 + datetime_.microsecond * 1000


def iso_time_format(datetime_: datetime) -> str:
    return "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ" % (
        datetime_.year,
        datetime_.month,
        datetime_.day,
        datetime_.hour,
        datetime_.minute,
        datetime_.second,
        int(datetime_.microsecond / 1000),
    )
