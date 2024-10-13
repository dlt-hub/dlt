from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.configuration.specs.pluggable_run_context import (
    PluggableRunContext,
    SupportsRunContext,
)

# telemetry should be initialized only once
_INITIALIZED = False


def initialize_runtime(
    run_context: SupportsRunContext, runtime_config: RuntimeConfiguration
) -> None:
    from dlt.sources.helpers import requests
    from dlt.common import logger
    from dlt.common.runtime.exec_info import dlt_version_info

    version = dlt_version_info(runtime_config.pipeline_name)

    # initialize or re-initialize logging with new settings
    logger.LOGGER = logger._create_logger(
        run_context.name,
        runtime_config.log_level,
        runtime_config.log_format,
        runtime_config.pipeline_name,
        version,
    )

    # Init or update default requests client config
    requests.init(runtime_config)


def restore_run_context(
    run_context: SupportsRunContext, runtime_config: RuntimeConfiguration
) -> None:
    """Restores `run_context` by placing it into container and if `runtime_config` is present, initializes runtime
    Intended to be called by workers in a process pool.
    """
    from dlt.common.configuration.container import Container

    Container()[PluggableRunContext] = PluggableRunContext(run_context, runtime_config)
    apply_runtime_config(runtime_config)
    init_telemetry(runtime_config)


def init_telemetry(runtime_config: RuntimeConfiguration) -> None:
    """Starts telemetry only once"""
    from dlt.common.runtime.telemetry import start_telemetry

    global _INITIALIZED
    # initialize only once
    if not _INITIALIZED:
        start_telemetry(runtime_config)
        _INITIALIZED = True


def apply_runtime_config(runtime_config: RuntimeConfiguration) -> None:
    """Updates run context with newest runtime_config"""
    from dlt.common.configuration.container import Container

    Container()[PluggableRunContext].initialize_runtime(runtime_config)
