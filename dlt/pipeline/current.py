"""Easy access to active pipelines, state, sources and schemas"""

from dlt.common.pipeline import source_state as _state, resource_state, get_current_pipe_name
from dlt.pipeline import pipeline as _pipeline
from dlt.extract.decorators import get_source_schema, get_source

pipeline = _pipeline
"""Alias for dlt.pipeline"""
state = source_state = _state
"""Alias for dlt.state"""
source_schema = get_source_schema
source = get_source
pipe_name = get_current_pipe_name
resource_name = get_current_pipe_name
