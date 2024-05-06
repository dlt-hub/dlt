import contextlib
import logging
import traceback
from logging import LogRecord, Logger
from typing import Any, Mapping, Iterator, Protocol

DLT_LOGGER_NAME = "dlt"
LOGGER: Logger = None


class LogMethod(Protocol):
    def __call__(self, msg: str, *args: Any, **kwds: Any) -> None: ...


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


def metrics(name: str, extra: Mapping[str, Any], stacklevel: int = 1) -> None:
    """Forwards metrics call to LOGGER"""
    if LOGGER:
        LOGGER.info(name, extra=extra, stacklevel=stacklevel)


@contextlib.contextmanager
def suppress_and_warn() -> Iterator[None]:
    try:
        yield
    except Exception:
        LOGGER.warning("Suppressed exception", exc_info=True)


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


class _MetricsFormatter(logging.Formatter):
    def format(self, record: LogRecord) -> str:  # noqa: A003
        from dlt.common.json import json

        s = super(_MetricsFormatter, self).format(record)
        # dump metrics dictionary nicely
        if "metrics" in record.__dict__:
            s = s + ": " + json.dumps(record.__dict__["metrics"])
        return s


def _init_logging(
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
