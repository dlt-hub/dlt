from dlt.common.configuration.specs import RunConfiguration

# telemetry should be initialized only once
_INITIALIZED = False
_RUN_CONFIGURATION: RunConfiguration = None


def initialize_runtime(config: RunConfiguration) -> None:
    from dlt.common.runtime.logger import init_logging
    from dlt.common.runtime.telemetry import start_telemetry
    from dlt.sources.helpers import requests

    global _INITIALIZED, _RUN_CONFIGURATION

    # initialize or re-initialize logging with new settings
    init_logging(config)

    # Init or update default requests client config
    requests.init(config)

    from exec_info import is_notebook


    if is_notebook():
        try:
            from magics import register_magics

            register_magics()
        except ImportError:
            pass

    # initialize only once
    if not _INITIALIZED:
        start_telemetry(config)
        _INITIALIZED = True

    # store last config
    _RUN_CONFIGURATION = config
