"""Easy access to active pipelines, state, sources and schemas"""

from dlt.common.pipeline import resource_state
from dlt.common.pipeline import source_state as _state
from dlt.extract.decorators import get_source_schema
from dlt.pipeline import pipeline as _pipeline

pipeline = _pipeline
"""Alias for dlt.pipeline"""
state = source_state = _state
"""Alias for dlt.state"""
source_schema = get_source_schema
