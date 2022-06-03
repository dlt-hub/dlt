import os
import logging
import tempfile
from typing import Any, Iterator, List, Sequence
from git import Repo, Git, RepositoryDirtyError
from contextlib import contextmanager

from dlt.common import json
from dlt.common.utils import uniq_id
from dlt.common.typing import StrAny, Optional
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


@contextmanager
def git_custom_key_command(private_key: Optional[str]) -> Iterator[str]:
    if private_key:
        key_file = tempfile.mktemp(prefix=uniq_id())
        with open(key_file, "w") as f:
            f.write(private_key)
        try:
            # permissions so SSH does not complain
            os.chmod(key_file, 0o600)
            yield 'ssh -o "StrictHostKeyChecking accept-new" -i %s' % key_file
        finally:
            os.remove(key_file)
    else:
        yield 'ssh -o "StrictHostKeyChecking accept-new"'


def ensure_remote_head(repo_path: str, with_git_command: Optional[str] = None) -> None:
    # update remotes and check if heads are same. ignores locally modified files
    repo = Repo(repo_path)
    # use custom environemnt if specified
    with repo.git.custom_environment(GIT_SSH_COMMAND=with_git_command):
        # update origin
        repo.remote().update()
        # get branch status
        status: str = repo.git.status("--short", "--branch", "-uno")
        # we expect first status line ## main...origin/main
        status_line = status.split("/n")[0]
        if not (status_line.startswith("##") and not status_line.endswith("]")):
            raise RepositoryDirtyError(repo, status)


def clone_repo(repository_url: str, clone_path: str, branch: Optional[str] = None, with_git_command: Optional[str] = None) -> None:
    repo = Repo.clone_from(repository_url, clone_path, env=dict(GIT_SSH_COMMAND=with_git_command))
    if branch:
        repo.git.checkout(branch)


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
                    global_args: Sequence[str] = None, command_args: Sequence[str] = None, vars: StrAny = None) -> Sequence[dbt_results.BaseResult]:
    args = ["--profiles-dir", profiles_dir]
    # add profile name if provided
    if profile_name:
        args += ["--profile", profile_name]
    # serialize dbt variables to pass to package
    if vars:
        args += ["--vars", json.dumps(vars)]
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