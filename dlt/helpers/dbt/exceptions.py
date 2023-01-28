from typing import Any, Sequence, NamedTuple

from dlt.common.exceptions import DltException


class DBTRunnerException(DltException):
    pass


class PrerequisitesException(DBTRunnerException):
    pass


class IncrementalSchemaOutOfSyncError(DBTRunnerException):
    pass


class DBTNodeResult(NamedTuple):
    model_name: str
    message: str
    time: float
    status: str


class DBTProcessingError(DBTRunnerException):
    def __init__(self, command: str, run_results: Sequence[DBTNodeResult], dbt_results: Any) -> None:
        self.command = command
        self.run_results = run_results
        # the results from DBT may be anything
        self.dbt_results = dbt_results
        super().__init__(f"DBT command {command} could not be executed")
