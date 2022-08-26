import threading
from typing import Type

from dlt.common import logger
from dlt.common.configuration.run_configuration import RunConfiguration
from dlt.common.logger import init_logging_from_config, init_telemetry
from dlt.common.signals import register_signals

# signals and telemetry should be initialized only once
_INITIALIZED = False


def initialize_runner(C: Type[RunConfiguration]) -> None:
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