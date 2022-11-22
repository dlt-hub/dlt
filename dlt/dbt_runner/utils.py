import os
import logging
from typing import Any, List, Sequence, Optional

from dlt.common import json
from dlt.common.typing import StrAny
from dlt.dbt_runner.exceptions import DBTRunnerException

# block disabling root logger
import logbook.compat
logbook.compat.redirect_logging = lambda : None

# can only import DBT after redirect is disabled
import dbt.main
import dbt.logger
from dbt.events import functions
from dbt.contracts import results as dbt_results
from dbt.exceptions import FailFastException


# keep this exception definition here due to mock of logbook
class DBTProcessingError(DBTRunnerException):
    def __init__(self, command: str, results: Any) -> None:
        self.command = command
        # the results from DBT may be anything
        self.results = results
        super().__init__(f"DBT command {command} could not be executed")


def initialize_dbt_logging(level: str, is_json_logging: bool) -> Sequence[str]:
    int_level = logging._nameToLevel[level]

    # wrap log setup to force out log level

    def setup_event_logger_wrapper(log_path: str, level_override:str = None) -> None:
        functions.setup_event_logger(log_path, level)
        # force log level as file is debug only
        functions.this.FILE_LOG.setLevel(level)
        functions.this.FILE_LOG.handlers[0].setLevel(level)

    dbt.main.setup_event_logger = setup_event_logger_wrapper

    globs = []
    if int_level <= logging.DEBUG:
        globs = ["--debug"]

    # return global parameters to be passed to setup logging

    if is_json_logging:
        return ["--log-format", "json"] + globs
    else:
        return globs


def is_incremental_schema_out_of_sync_error(error: dbt_results.RunResult) -> bool:
    return issubclass(type(error), dbt_results.RunResult) and error.status == dbt_results.RunStatus.Error and\
        "The source and target schemas on this incremental model are out of sync" in error.message


def run_dbt_command(package_path: str, command: str, profiles_dir: str, profile_name: Optional[str] = None,
                    global_args: Sequence[str] = None, command_args: Sequence[str] = None, dbt_vars: StrAny = None) -> Sequence[dbt_results.BaseResult]:
    args = ["--profiles-dir", profiles_dir]
    # add profile name if provided
    if profile_name:
        args += ["--profile", profile_name]
    # serialize dbt variables to pass to package
    if dbt_vars:
        args += ["--vars", json.dumps(dbt_vars)]
    if command_args:
        args += command_args

    # cwd to package dir
    working_dir = os.getcwd()
    os.chdir(package_path)
    try:
        results: List[dbt_results.BaseResult] = None
        success: bool = None
        results, success = dbt.main.handle_and_check((global_args or []) + [command] + args)  # type: ignore
        assert type(success) is bool
        if not success:
            raise DBTProcessingError(command ,results)
        return results
    except FailFastException as ff:
        raise DBTProcessingError(command, ff.result) from ff
    finally:
        # unblock logger manager to run next command
        dbt.logger.log_manager.reset_handlers()
        # go back to working dir
        os.chdir(working_dir)