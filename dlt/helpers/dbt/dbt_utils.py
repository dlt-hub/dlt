import os
import logging
from typing import Any, Sequence, Optional, Union
import warnings

from dlt.common import logger
from dlt.common.json import json
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import StrAny

from dlt.helpers.dbt.exceptions import (
    DBTProcessingError,
    DBTNodeResult,
    IncrementalSchemaOutOfSyncError,
)

try:
    # block disabling root logger
    import logbook.compat

    logbook.compat.redirect_logging = lambda: None

    # can only import DBT after redirect is disabled
    # https://stackoverflow.com/questions/48619517/call-a-click-command-from-code

    import dbt.logger
    from dbt.contracts import results as dbt_results
except ModuleNotFoundError:
    raise MissingDependencyException("DBT Core", ["dbt-core"])

try:
    # dbt <1.5
    from dbt.main import handle_and_check  # type: ignore[import-not-found]
except ImportError:
    # dbt >=1.5
    from dbt.cli.main import dbtRunner

try:
    from dbt.exceptions import FailFastException  # type: ignore
except ImportError:
    from dbt.exceptions import FailFastError as FailFastException

_DBT_LOGGER_INITIALIZED = False


def initialize_dbt_logging(level: str, is_json_logging: bool) -> Sequence[str]:
    int_level = logging._nameToLevel[level]

    # wrap log setup to force out log level

    def set_path_wrapper(self: dbt.logger.LogManager, path: str) -> None:
        global _DBT_LOGGER_INITIALIZED

        if not _DBT_LOGGER_INITIALIZED:
            self._file_handler.set_path(path)
        _DBT_LOGGER_INITIALIZED = True

    dbt.logger.LogManager.set_path = set_path_wrapper  # type: ignore

    globs = []
    if int_level <= logging.DEBUG:
        globs = ["--debug"]
    if int_level >= logging.WARNING:
        globs = ["--quiet", "--no-print"]

    # return global parameters to be passed to setup logging

    if is_json_logging:
        return ["--log-format", "json"] + globs
    else:
        return globs


def is_incremental_schema_out_of_sync_error(error: Any) -> bool:
    def _check_single_item(error_: dbt_results.RunResult) -> bool:
        return (
            error_.status == dbt_results.RunStatus.Error
            and "The source and target schemas on this incremental model are out of sync"
            in error_.message
        )

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
        DBTNodeResult(res.node.name, res.message, res.execution_time, str(res.status))
        for res in results
        if isinstance(res, dbt_results.NodeResult)
    ]


def run_dbt_command(
    package_path: str,
    command: str,
    profiles_dir: str,
    profile_name: Optional[str] = None,
    global_args: Sequence[str] = None,
    command_args: Sequence[str] = None,
    package_vars: StrAny = None,
) -> Union[Sequence[DBTNodeResult], dbt_results.ExecutionResult]:
    args = ["--profiles-dir", profiles_dir]
    # add profile name if provided
    if profile_name:
        args += ["--profile", profile_name]
    # serialize dbt variables to pass to package
    if package_vars:
        args += ["--vars", json.dumps(package_vars)]
    if command_args:
        args += command_args

    # cwd to package dir
    working_dir = os.getcwd()
    os.chdir(package_path)
    try:
        results: dbt_results.ExecutionResult = None
        success: bool = None
        # dbt uses logbook which does not run on python 10. below is a hack that allows that
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="logbook")
        runner_args = (global_args or []) + [command] + args  # type: ignore

        with dbt.logger.log_manager.applicationbound():
            try:
                # dbt 1.5
                runner = dbtRunner()
                run_result = runner.invoke(runner_args)
                success = run_result.success
                results = run_result.result  # type: ignore
            except NameError:
                # dbt < 1.5
                results, success = handle_and_check(runner_args)

        assert type(success) is bool
        parsed_results = parse_dbt_execution_results(results)
        if not success:
            dbt_exc = DBTProcessingError(command, parsed_results, results)
            # detect incremental model out of sync
            if is_incremental_schema_out_of_sync_error(results):
                raise IncrementalSchemaOutOfSyncError(dbt_exc)
            raise dbt_exc
        return parsed_results or results
    except SystemExit as sys_ex:
        # oftentimes dbt tries to exit on error
        raise DBTProcessingError(command, None, sys_ex)
    except FailFastException as ff:
        dbt_exc = DBTProcessingError(command, parse_dbt_execution_results(ff.result), ff.result)
        # detect incremental model out of sync
        if is_incremental_schema_out_of_sync_error(ff.result):
            raise IncrementalSchemaOutOfSyncError(dbt_exc) from ff
        raise dbt_exc from ff
    finally:
        # go back to working dir
        os.chdir(working_dir)


def init_logging_and_run_dbt_command(
    log_level: str,
    is_json_logging: bool,
    package_path: str,
    command: str,
    profiles_dir: str,
    profile_name: Optional[str] = None,
    command_args: Sequence[str] = None,
    package_vars: StrAny = None,
) -> Union[Sequence[DBTNodeResult], dbt_results.ExecutionResult]:
    # initialize dbt logging, returns global parameters to dbt command
    dbt_global_args = initialize_dbt_logging(log_level, is_json_logging)
    return run_dbt_command(
        package_path,
        command,
        profiles_dir,
        profile_name,
        dbt_global_args,
        command_args,
        package_vars,
    )
