import os
import logging
from typing import Any, Sequence, Optional, Union
import warnings

from dlt.common import logger
from dlt.common.json import json
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import StrAny, TypeAlias, AnyType

from dlt.helpers.dbt.exceptions import (
    DBTProcessingError,
    DBTNodeResult,
    IncrementalSchemaOutOfSyncError,
)


try:
    from dbt.contracts import results as dbt_results
    from dbt.cli.main import dbtRunner
except ImportError:
    raise MissingDependencyException("DBT Core", ["dbt-core"])


def initialize_dbt_logging(level: str, is_json_logging: bool) -> Sequence[str]:
    int_level = logging._nameToLevel[level]

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

        runner = dbtRunner()
        run_result = runner.invoke(runner_args)
        success = run_result.success
        results = run_result.result  # type: ignore

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
        # NOTE: current implementation handles BaseExceptions so in theory SystemExit will not come here

        raise DBTProcessingError(command, None, sys_ex)
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
