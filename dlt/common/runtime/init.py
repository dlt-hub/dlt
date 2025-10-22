from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.configuration.specs.pluggable_run_context import (
    PluggableRunContext,
    RunContextBase,
)


def initialize_runtime(logger_name: str, runtime_config: RuntimeConfiguration) -> None:
    from dlt.common import logger
    from dlt.common.runtime.exec_info import dlt_version_info
    from dlt.common.runtime.telemetry import start_telemetry

    from dlt.sources.helpers import requests

    version = dlt_version_info(runtime_config.pipeline_name)

    # initialize or re-initialize logging with new settings
    logger.LOGGER = logger._create_logger(
        logger_name,
        runtime_config.log_level,
        runtime_config.log_format,
        runtime_config.pipeline_name,
        version,
    )

    # Init or update default requests client config
    requests.init(runtime_config)
    # initialize telemetry
    start_telemetry(runtime_config)


def restore_run_context(run_context: RunContextBase) -> None:
    """Restores `run_context` by placing it into container and if `runtime_config` is present, initializes runtime
    Intended to be called by workers in a process pool.
    """
    from dlt.common.configuration.container import Container

    # make sure runtime configuration is attached
    assert run_context.runtime_config is not None

    Container()[PluggableRunContext] = PluggableRunContext(run_context)
