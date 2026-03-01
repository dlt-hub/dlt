from typing import Dict, Optional, Type

from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext
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


def restore_run_context(
    run_context: RunContextBase,
    worker_contexts: Optional[
        Dict[Type[ContainerInjectableContext], ContainerInjectableContext]
    ] = None,
) -> None:
    """Restores run context and worker-affinity contexts in a worker process.

    Called by process pool workers to restore the execution environment from the
    main process. The `run_context` is used to create a new `PluggableRunContext`,
    while `worker_contexts` are contexts marked with `worker_affinity=True` that
    are placed directly into the container.

    Args:
        run_context: The RunContextBase from the main process. Must have
            `runtime_config` attached.
        worker_contexts: Dict mapping context types to instances, collected via
            `Container.get_worker_contexts()`. Each context is placed into the
            worker's container under its type key.
    """
    from dlt.common.configuration.container import Container

    # make sure runtime configuration is attached
    assert run_context.runtime_config is not None

    container = Container()
    container[PluggableRunContext] = PluggableRunContext(run_context)

    if worker_contexts:
        for spec, ctx in worker_contexts.items():
            container[spec] = ctx
