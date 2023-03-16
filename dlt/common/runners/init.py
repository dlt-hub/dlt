import threading

from dlt.common import logger
from dlt.common.runtime.logger import init_logging
from dlt.common.runtime.telemetry import start_telemetry
from dlt.common.runtime.signals import register_signals
from dlt.common.configuration.specs import RunConfiguration

# signals and telemetry should be initialized only once
_INITIALIZED = False
_RUN_CONFIGURATION: RunConfiguration = None


def initialize_runner(config: RunConfiguration) -> None:
    global _INITIALIZED, _RUN_CONFIGURATION

    # initialize or re-initialize logging with new settings
    init_logging(config)

    # initialize only once
    if not _INITIALIZED:
        start_telemetry(config)

        if threading.current_thread() is threading.main_thread():
            register_signals()
        else:
            logger.info("Running in daemon thread, signals not enabled")

        _INITIALIZED = True

    # store last config
    _RUN_CONFIGURATION = config
