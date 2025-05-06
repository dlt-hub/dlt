from typing import Literal
from typing_extensions import ParamSpec

TTransformationFunParams = ParamSpec("TTransformationFunParams")

TLineageMode = Literal["strict", "best_effort", "disabled"]
"""
defines the mode of lineage collection for transformations
- strict: collect lineage for all columns, will fail if any column can not be resolved
- best_effort: collect lineage for all columns that can be resolved (default)
- off: disable lineage collection, no hints are forwarded to the destination
"""
