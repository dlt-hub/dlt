"""Easy access to active pipelines, state, sources and schemas"""

from dlt.common.pipeline import source_state as _state, resource_state, get_current_pipe_name
from dlt.pipeline.pipeline import Pipeline
from dlt.extract.decorators import get_source_schema
from dlt.common.storages.load_package import (
    load_package,
    commit_load_package_state,
    destination_state,
    clear_destination_state,
)
from dlt.extract.decorators import get_source_schema, get_source


def pipeline() -> Pipeline:
    """Currently active pipeline ie. the most recently created or run"""
    from dlt import _pipeline

    return _pipeline()


state = source_state = _state
source_schema = get_source_schema
source = get_source
pipe_name = get_current_pipe_name
resource_name = get_current_pipe_name
