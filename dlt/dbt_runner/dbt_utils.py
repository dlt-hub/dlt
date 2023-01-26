import os
import logging
from typing import Any, Sequence, Optional

from dlt.common import json, logger
from dlt.common.exceptions import MissingDependencyException
from dlt.common.runners.synth_pickle import encode_obj
from dlt.common.typing import StrAny

from dlt.dbt_runner.exceptions import DBTProcessingError, DBTNodeResult

try:
    # block disabling root logger
    import logbook.compat
    logbook.compat.redirect_logging = lambda : None

    # can only import DBT after redirect is disabled
    import dbt.main
    import dbt.logger
    from dbt.events import functions
    from dbt.contracts import results as dbt_results
    from dbt.exceptions import FailFastException
except ImportError:
    raise MissingDependencyException("DBT Core", ["dbt-core"])


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


def is_incremental_schema_out_of_sync_error(error: Any) -> bool:
    # print("IS_INCREMENTAL_SCHEMA_OUT_OF_SYNC_ERROR")
    # print(error)
    # print("X:" + encode_obj(error, ignore_pickle_errors=False))
    # print("LINE?")

    def _check_single_item(error_: dbt_results.RunResult) -> bool:
        return error_.status == dbt_results.RunStatus.Error and "The source and target schemas on this incremental model are out of sync" in error_.message

    if isinstance(error, dbt_results.RunResult):
        return _check_single_item(error)
    elif isinstance(error, dbt_results.RunExecutionResult):
        return any(_check_single_item(r) for r in error.results)

    return False


def parse_dbt_execution_results(results: Any) -> Sequence[DBTNodeResult]:
    # run may return RunResult of something different depending on error
    if isinstance(results, dbt_results.ExecutionResult):
        pass
    elif isinstance(results, dbt_results.BaseResult):
        # a single result
        results = [results]
    else:
        logger.warning(f"{type(results)} is unknown and cannot be logged")
        return None

    return [
            DBTNodeResult(res.node.name, res.message, res.execution_time, str(res.status)) for res in results if isinstance(res, dbt_results.NodeResult)
        ]


def run_dbt_command(package_path: str, command: str, profiles_dir: str, profile_name: Optional[str] = None,
                    global_args: Sequence[str] = None, command_args: Sequence[str] = None, dbt_vars: StrAny = None) -> dbt_results.ExecutionResult:
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
        results: dbt_results.ExecutionResult = None
        success: bool = None
        results, success = dbt.main.handle_and_check((global_args or []) + [command] + args)  # type: ignore
        assert type(success) is bool
        if not success:
            raise DBTProcessingError(command, parse_dbt_execution_results(results), results)
        return results
    except SystemExit as sys_ex:
        raise DBTProcessingError(command, None, sys_ex)
    except FailFastException as ff:
        raise DBTProcessingError(command, parse_dbt_execution_results(ff.result), ff.result) from ff
    finally:
        # unblock logger manager to run next command
        dbt.logger.log_manager.reset_handlers()
        # go back to working dir
        os.chdir(working_dir)