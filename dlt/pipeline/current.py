"""Easy access to active pipelines, state, sources and schemas"""

from dlt.common.pipeline import state as _state
from dlt.pipeline import pipeline as _pipeline
from dlt.extract.decorators import get_source_schema

pipeline = _pipeline
"""Alias for dlt.pipeline"""
state = _state
"""Alias for dlt.state"""
source_schema = get_source_schema

