"""Easy access to active pipelines, state, sources and schemas"""

from dlt.common.storages.load_package import (
    load_package_state,
    destination_state,
)
from dlt.common.runtime.run_context import active as _run_context

from dlt.extract.decorators import (
    get_source_schema as _get_source_schema,
    get_source as _get_source,
)
from dlt.extract.state import (
    source_state,
    resource_state,
    get_current_pipe_name as _get_current_pipe_name,
    get_current_pipe as _get_current_pipe,
)

from dlt.pipeline.pipeline import Pipeline as _Pipeline


def pipeline() -> _Pipeline:
    """Currently active pipeline ie. the most recently created or run"""
    from dlt import _pipeline

    return _pipeline()


state = source_state
source_schema = _get_source_schema
source = _get_source
pipe = _get_current_pipe
resource_name = _get_current_pipe_name
run_context = _run_context

__all__ = [
    "load_package_state",
    "destination_state",
    "source_state",
    "resource_state",
    "source_schema",
    "pipe",
    "resource_name",
    "run_context",
]
