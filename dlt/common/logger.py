import contextlib
import logging
import traceback
from logging import LogRecord, Logger
from typing import Any, Mapping, Iterator, Protocol, Callable

LOGGER: Logger = None
_MESSAGE_LOGGING_METHODS = {
    "debug",
    "info",
    "warning",
    "warn",
    "error",
    "critical",
    "exception",
    "fatal",
}


class LogMethod(Protocol):
    def __call__(self, *args: Any, **kwds: Any) -> Any: ...


def __getattr__(name: str) -> LogMethod:
    """Forwards log method calls (debug, info, error etc.) to LOGGER"""

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if LOGGER is None:
            return None

        target: Callable[..., Any] = getattr(LOGGER, name)

        if name in _MESSAGE_LOGGING_METHODS:
            # skip stack frames when displaying log so the original logging frame is displayed
            stacklevel = 2
            if name == "exception":
                # exception has one more frame
                stacklevel = 3
            kwargs["stacklevel"] = stacklevel
        return target(*args, **kwargs)

    wrapper.__name__ = name
    return wrapper


def metrics(name: str, extra: Mapping[str, Any], stacklevel: int = 1) -> None:
    """Forwards metrics call to LOGGER"""
    if LOGGER:
        LOGGER.info(name, extra=extra, stacklevel=stacklevel)


@contextlib.contextmanager
def suppress_and_warn(msg: str) -> Iterator[None]:
    try:
        yield
    except Exception:
        LOGGER.warning(msg, exc_info=True)


def is_logging() -> bool:
    return LOGGER is not None


def log_level() -> str:
    if not LOGGER:
        raise RuntimeError("Logger not initialized")
    return logging.getLevelName(LOGGER.level)


def is_json_logging(log_format: str) -> bool:
    return log_format == "JSON"


def pretty_format_exception() -> str:
    return traceback.format_exc()


class _MetricsFormatter(logging.Formatter):
    def format(self, record: LogRecord) -> str:  # noqa: A003
        from dlt.common.json import json

        s = super(_MetricsFormatter, self).format(record)
        # dump metrics dictionary nicely
        if "metrics" in record.__dict__:
            s = s + ": " + json.dumps(record.__dict__["metrics"])
        return s


def _create_logger(
    logger_name: str, level: str, fmt: str, component: str, version: Mapping[str, str]
) -> Logger:
    if logger_name == "root":
        logging.basicConfig(level=level)
        handler = logging.getLogger().handlers[0]
        logger = logging.getLogger()
    else:
        logger = logging.getLogger(logger_name)
        logger.propagate = False
        logger.setLevel(level)
        # get or create logging handler, we log to stderr by default
        handler = next(iter(logger.handlers), logging.StreamHandler())
        logger.addHandler(handler)

    # set right formatter
    if is_json_logging(fmt):
        from dlt.common.runtime import json_logging

        class _CustomJsonFormatter(json_logging.JSONLogFormatter):
            version: Mapping[str, str] = None

            def _format_log_object(self, record: LogRecord) -> Any:
                json_log_object = super(_CustomJsonFormatter, self)._format_log_object(record)
                if self.version:
                    json_log_object.update({"version": self.version})
                return json_log_object

        json_logging.COMPONENT_NAME = component
        if "process" in json_logging.RECORD_ATTR_SKIP_LIST:
            json_logging.RECORD_ATTR_SKIP_LIST.remove("process")
        # set version as class variable as we cannot pass custom constructor parameters
        _CustomJsonFormatter.version = version
        # the only thing method above effectively does is to replace the formatter
        json_logging.init(custom_formatter=_CustomJsonFormatter)
        if logger_name == "root":
            json_logging.config_root_logger()
    else:
        handler.setFormatter(_MetricsFormatter(fmt=fmt, style="{"))

    return logger


def _delete_current_logger() -> None:
    if not LOGGER:
        return

    for handler in LOGGER.handlers[:]:
        LOGGER.removeHandler(handler)

    LOGGER.disabled = True
    LOGGER.propagate = False
