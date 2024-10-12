import logging

from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.configuration.specs.pluggable_run_context import (
    PluggableRunContext,
    SupportsRunContext,
)

# telemetry should be initialized only once
_INITIALIZED = False


def init_logging(run_context: SupportsRunContext) -> logging.Logger:
    from dlt.common import logger
    from dlt.common.runtime.exec_info import dlt_version_info

    config = run_context.runtime_config
    version = dlt_version_info(config.pipeline_name)
    return logger._create_logger(
        run_context.name or logger.DLT_LOGGER_NAME,
        config.log_level,
        config.log_format,
        config.pipeline_name,
        version,
    )


def restore_run_context(run_context: SupportsRunContext) -> None:
    """Restores `run_context` by placing it into container and if `runtime_config` is present, initializes runtime
    Intended top be called by workers in process pool.
    """
    from dlt.common.configuration.container import Container

    Container()[PluggableRunContext] = PluggableRunContext(run_context)
    if run_context.runtime_config:
        initialize_runtime(run_context.runtime_config)


def initialize_runtime(config: RuntimeConfiguration) -> None:
    from dlt.common.configuration.container import Container
    from dlt.common.runtime.telemetry import start_telemetry
    from dlt.sources.helpers import requests

    global _INITIALIZED

    # initialize or re-initialize logging with new settings
    Container()[PluggableRunContext].init_runtime(config)

    # Init or update default requests client config
    requests.init(config)

    # initialize only once
    if not _INITIALIZED:
        start_telemetry(config)
        _INITIALIZED = True
