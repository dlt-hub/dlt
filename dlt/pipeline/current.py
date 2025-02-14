"""Easy access to active pipelines, state, sources and schemas"""

from dlt.common.pipeline import source_state as _state, resource_state, get_current_pipe_name
from dlt.common.storages.load_package import (
    load_package,
    commit_load_package_state,
    destination_state,
    clear_destination_state,
)
from dlt.common.runtime.run_context import active as _run_context

from dlt.extract.decorators import get_source_schema, get_source
from dlt.pipeline.pipeline import Pipeline as _Pipeline


def pipeline() -> _Pipeline:
    """Currently active pipeline ie. the most recently created or run"""
    from dlt import _pipeline

    return _pipeline()


state = source_state = _state
source_schema = get_source_schema
source = get_source
pipe_name = get_current_pipe_name
resource_name = get_current_pipe_name
run_context = _run_context
