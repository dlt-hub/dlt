import contextlib
import logging
import json_logging
import traceback
from logging import LogRecord, Logger
from typing import Any, Iterator, Literal, Protocol

from dlt.common.json import json
from dlt.common.runtime.exec_info import dlt_version_info
from dlt.common.typing import StrAny, StrStr
from dlt.common.configuration.specs import RunConfiguration

DLT_LOGGER_NAME = "dlt"
LOGGER: Logger = None
TMetricsCategory = Literal["start", "progress", "stop"]


class LogMethod(Protocol):
    def __call__(self, msg: str, *args: Any, **kwds: Any) -> None:
        ...


def __getattr__(name: str) -> LogMethod:
    """Forwards log method calls (debug, info, error etc.) to LOGGER"""
    def wrapper(msg: str, *args: Any, **kwargs: Any) -> None:
        if LOGGER:
            # skip stack frames when displaying log so the original logging frame is displayed
            stacklevel = 2
            if name == "exception":
                # exception has one more frame
                stacklevel = 3
            getattr(LOGGER, name)(msg, *args, **kwargs, stacklevel=stacklevel)
    return wrapper


def metrics(category: TMetricsCategory, name: str, extra: StrAny, stacklevel: int = 1) -> None:
    """Forwards metrics call to LOGGER"""
    if LOGGER:
        LOGGER.metrics(f"{category}:{name}", extra=extra, stacklevel=stacklevel)  # type: ignore


@contextlib.contextmanager
def suppress_and_warn() -> Iterator[None]:
    try:
        yield
    except Exception:
        LOGGER.warning("Suppressed exception", exc_info=True)


def init_logging(config: RunConfiguration) -> None:
    global LOGGER

    # add HEALTH and METRICS log levels
    if not hasattr(logging, "metrics"):
        # _add_logging_level("HEALTH", logging.WARNING - 1, "health")
        _add_logging_level("METRICS", logging.WARNING - 2, "metrics")

    version = dlt_version_info(config)
    LOGGER = _init_logging(
        DLT_LOGGER_NAME,
        config.log_level,
        config.log_format,
        config.pipeline_name,
        version)


def is_logging() -> bool:
    return LOGGER is not None


def log_level() -> str:
    if not LOGGER:
        raise RuntimeError("Logger not initialized")
    return logging.getLevelName(LOGGER.level)  # type: ignore


def is_json_logging(log_format: str) -> bool:
    return log_format == "JSON"


def pretty_format_exception() -> str:
    return traceback.format_exc()


def _add_logging_level(level_name: str, level: int, method_name:str = None) -> None:
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    """
    if not method_name:
        method_name = level_name.lower()

    if hasattr(logging, level_name):
        raise AttributeError('{} already defined in logging module'.format(level_name))
    if hasattr(logging, method_name):
        raise AttributeError('{} already defined in logging module'.format(method_name))
    if hasattr(logging.getLoggerClass(), method_name):
        raise AttributeError('{} already defined in logger class'.format(method_name))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self: logging.Logger, message: str, *args: Any, **kwargs: Any) -> None:
        if self.isEnabledFor(level):
            self._log(level, message, args, **kwargs)
    def logToRoot(message: str, *args: Any, **kwargs: Any) -> None:
        logging.root._log(level, message, args, **kwargs)

    logging.addLevelName(level, level_name)
    setattr(logging, level_name, level)
    setattr(logging.getLoggerClass(), method_name, logForLevel)
    setattr(logging, method_name, logToRoot)


class _MetricsFormatter(logging.Formatter):
    def format(self, record: LogRecord) -> str:  # noqa: A003
        s = super(_MetricsFormatter, self).format(record)
        # dump metrics dictionary nicely
        if "metrics" in record.__dict__:
            s = s + ": " + json.dumps(record.__dict__["metrics"])
        return s


class _CustomJsonFormatter(json_logging.JSONLogFormatter):

    version: StrStr = None

    def _format_log_object(self, record: LogRecord, request_util: Any) -> Any:
        json_log_object = super(_CustomJsonFormatter, self)._format_log_object(record, request_util)
        if self.version:
            json_log_object.update({"version": self.version})
        return json_log_object


def _init_logging(logger_name: str, level: str, fmt: str, component: str, version: StrStr) -> Logger:
    if logger_name == "root":
        logging.basicConfig(level=level)
        handler = logging.getLogger().handlers[0]
        # handler.setFormatter(_MetricsFormatter(fmt=format, style='{'))
        logger = logging.getLogger()
    else:
        logger = logging.getLogger(DLT_LOGGER_NAME)
        logger.propagate = False
        logger.setLevel(level)
        # get or create logging handler
        handler = next(iter(logger.handlers), logging.StreamHandler())
        logger.addHandler(handler)

    # set right formatter
    if is_json_logging(fmt):
        json_logging.COMPONENT_NAME = component
        json_logging.JSON_SERIALIZER = json.dumps
        json_logging.RECORD_ATTR_SKIP_LIST.remove("process")
        # set version as class variable as we cannot pass custom constructor parameters
        _CustomJsonFormatter.version = version
        # the only thing method above effectively does is to replace the formatter
        json_logging.init_non_web(enable_json=True, custom_formatter=_CustomJsonFormatter)
        if logger_name == "root":
            json_logging.config_root_logger()
    else:
        handler.setFormatter(_MetricsFormatter(fmt=fmt, style='{'))

    return logger
