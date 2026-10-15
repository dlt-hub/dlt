from typing import Any, Dict, Optional, Sequence, NamedTuple

from dlt.common.exceptions import DltException


class DBTRunnerException(DltException):
    pass


class PrerequisitesException(DBTRunnerException):
    pass


class IncrementalSchemaOutOfSyncError(DBTRunnerException):
    pass


class DBTNodeResult(NamedTuple):
    model_name: str
    """Name of the dbt node: a model, seed or test"""
    message: str
    """Message that dbt reports for the node, for example `SELECT 10`"""
    time: float
    """Execution time of the node in seconds"""
    status: str
    """Status that dbt reports for the node, for example `success`, `error`, `pass` or `warn`"""
    load_id: Optional[str] = None
    """Load id passed to the package as `load_id` variable"""
    rows_affected: Optional[int] = None
    """Rows affected by the main statement of the node as reported by the dbt adapter"""
    failures: Optional[int] = None
    """Number of failing rows of a data test"""
    adapter_response: Optional[Dict[str, Any]] = None
    """Raw response of the dbt adapter, content depends on the adapter"""


class DBTProcessingError(DBTRunnerException):
    def __init__(
        self, command: str, run_results: Sequence[DBTNodeResult], dbt_results: Any
    ) -> None:
        self.command = command
        self.run_results = run_results
        # the results from DBT may be anything
        self.dbt_results = dbt_results
        super().__init__(f"DBT command `{command}` could not be executed")
