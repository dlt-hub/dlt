from typing import Literal
from typing_extensions import ParamSpec

TTransformationType = Literal["python", "sql"]
TTransformationGroupType = Literal["dbt", "python", "sql"]
"""Type of transformation
- dbt: use a dbt transformation
- python: use a python function, data is extracted into memory
    and then loaded via a dlt pipeline into the destination dataset
- python-duckdb: use a python function, data is extracted into memory
    and loaded directly into a duckdb instance without a pipeline
- sql: use a sql or ibis expression, transformation is run directly on the database
"""

TTransformationFunParams = ParamSpec("TTransformationFunParams")
TTransformationGroupFunParams = ParamSpec("TTransformationGroupFunParams")


TLineageMode = Literal["strict", "best_effort", "disabled"]
"""
defines the mode of lineage collection for transformations
- strict: collect lineage for all columns, will fail if any column can not be resolved
- best_effort: collect lineage for all columns that can be resolved (default)
- off: disable lineage collection, no hints are forwarded to the destination
"""
