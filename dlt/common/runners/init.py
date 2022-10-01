import threading

from dlt.common import logger
from dlt.common.logger import init_logging_from_config, init_telemetry
from dlt.common.signals import register_signals
from dlt.common.configuration.specs import RunConfiguration

# signals and telemetry should be initialized only once
_INITIALIZED = False


def initialize_runner(C: RunConfiguration) -> None:
    global _INITIALIZED

    # initialize or re-initialize logging with new settings
    init_logging_from_config(C)

    # initialize only once
    if not _INITIALIZED:
        init_telemetry(C)

        if threading.current_thread() is threading.main_thread():
            register_signals()
        else:
            logger.info("Running in daemon thread, signals not enabled")

        _INITIALIZED = True