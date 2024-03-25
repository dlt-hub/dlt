import os
from subprocess import CalledProcessError
import giturlparse
from typing import Sequence

import dlt
from dlt.common import logger
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.utils import add_config_to_env
from dlt.common.destination.reference import DestinationClientDwhConfiguration
from dlt.common.runners import Venv
from dlt.common.runners.stdout import iter_stdout_with_result
from dlt.common.typing import StrAny, TSecretValue
from dlt.common.logger import is_json_logging
from dlt.common.storages import FileStorage
from dlt.common.git import git_custom_key_command, ensure_remote_head, force_clone_repo
from dlt.common.utils import with_custom_environ

from dlt.helpers.dbt.configuration import DBTRunnerConfiguration
from dlt.helpers.dbt.exceptions import (
    IncrementalSchemaOutOfSyncError,
    PrerequisitesException,
    DBTNodeResult,
    DBTProcessingError,
)

from dlt.common.runtime.telemetry import with_telemetry


class DBTPackageRunner:
    """A Python wrapper over a dbt package

    The created wrapper minimizes the required effort to run `dbt` packages on datasets created with `dlt`. It clones the package repo and keeps it up to data,
    shares the `dlt` destination credentials with `dbt` and allows the isolated execution with `venv` parameter.
    The wrapper creates a `dbt` profile from a passed `dlt` credentials and executes the transformations in `source_dataset_name` schema. Additional configuration is
    passed via DBTRunnerConfiguration instance
    """

    def __init__(
        self,
        venv: Venv,
        credentials: DestinationClientDwhConfiguration,
        working_dir: str,
        source_dataset_name: str,
        config: DBTRunnerConfiguration,
    ) -> None:
        self.venv = venv
        self.credentials = credentials
        self.working_dir = working_dir
        self.source_dataset_name = source_dataset_name
        self.config = config

        self.package_path: str = None
        # set if package is in repo
        self.repo_storage: FileStorage = None
        self.cloned_package_name: str = None

        self._setup_location()

    def _setup_location(self) -> None:
        # set the package location
        url = giturlparse.parse(self.config.package_location, check_domain=False)
        if not url.valid:
            self.package_path = self.config.package_location
        else:
            # location is a repository
            self.repo_storage = FileStorage(self.working_dir, makedirs=True)
            self.cloned_package_name = url.name
            self.package_path = os.path.join(self.working_dir, self.cloned_package_name)

    def _get_package_vars(
        self, additional_vars: StrAny = None, destination_dataset_name: str = None
    ) -> StrAny:
        if self.config.package_additional_vars:
            package_vars = dict(self.config.package_additional_vars)
        else:
            package_vars = {}
        package_vars["source_dataset_name"] = self.source_dataset_name
        if destination_dataset_name:
            package_vars["destination_dataset_name"] = destination_dataset_name
        if additional_vars:
            package_vars.update(additional_vars)
        return package_vars

    def _log_dbt_run_results(self, results: Sequence[DBTNodeResult]) -> None:
        if not results:
            return

        for res in results:
            if res.status == "error":
                logger.error(f"Model {res.model_name} error! Error: {res.message}")
            else:
                logger.info(
                    f"Model {res.model_name} {res.status} in {res.time} seconds with {res.message}"
                )

    def ensure_newest_package(self) -> None:
        """Clones or brings the dbt package at `package_location` up to date."""
        from git import GitError

        with git_custom_key_command(self.config.package_repository_ssh_key) as ssh_command:
            try:
                ensure_remote_head(
                    self.package_path,
                    branch=self.config.package_repository_branch,
                    with_git_command=ssh_command,
                )
            except GitError as err:
                # cleanup package folder
                logger.info(f"Package will be cloned due to {type(err).__name__}:{str(err)}")
                logger.info(
                    f"Will clone {self.config.package_location} head"
                    f" {self.config.package_repository_branch} into {self.package_path}"
                )
                force_clone_repo(
                    self.config.package_location,
                    self.repo_storage,
                    self.cloned_package_name,
                    self.config.package_repository_branch,
                    with_git_command=ssh_command,
                )

    @with_custom_environ
    def _run_dbt_command(
        self, command: str, command_args: Sequence[str] = None, package_vars: StrAny = None
    ) -> Sequence[DBTNodeResult]:
        logger.info(
            f"Exec dbt command: {command} {command_args} {package_vars} on profile"
            f" {self.config.package_profile_name}"
        )
        # write credentials to environ to pass them to dbt, add DLT__ prefix
        if self.credentials:
            add_config_to_env(self.credentials, ("dlt",))
        args = [
            self.config.runtime.log_level,
            is_json_logging(self.config.runtime.log_format),
            self.package_path,
            command,
            self.config.package_profiles_dir,
            self.config.package_profile_name,
            command_args,
            package_vars,
        ]
        script = f"""
from functools import partial

from dlt.common.runners.stdout import exec_to_stdout
from dlt.helpers.dbt.dbt_utils import init_logging_and_run_dbt_command

f = partial(init_logging_and_run_dbt_command, {", ".join(map(lambda arg: repr(arg), args))})
with exec_to_stdout(f):
    pass
"""
        try:
            i = iter_stdout_with_result(self.venv, "python", "-c", script)
            while True:
                print(next(i).strip())
        except StopIteration as si:
            # return result from generator
            return si.value  # type: ignore
        except CalledProcessError as cpe:
            print(cpe.stderr)
            raise

    def run(
        self,
        cmd_params: Sequence[str] = ("--fail-fast",),
        additional_vars: StrAny = None,
        destination_dataset_name: str = None,
    ) -> Sequence[DBTNodeResult]:
        """Runs `dbt` package

        Executes `dbt run` on previously cloned package.

        Args:
            run_params (Sequence[str], optional): Additional parameters to `run` command ie. `full-refresh`. Defaults to ("--fail-fast", ).
            additional_vars (StrAny, optional): Additional jinja variables to be passed to the package. Defaults to None.
            destination_dataset_name (str, optional): Overwrites the dbt schema where transformed models will be created. Useful for testing or creating several copies of transformed data . Defaults to None.

        Returns:
            Sequence[DBTNodeResult]: A list of processed model with names, statuses, execution messages and execution times

        Exceptions:
            DBTProcessingError: `run` command failed. Contains a list of models with their execution statuses and error messages
        """
        return self._run_dbt_command(
            "run", cmd_params, self._get_package_vars(additional_vars, destination_dataset_name)
        )

    def test(
        self,
        cmd_params: Sequence[str] = None,
        additional_vars: StrAny = None,
        destination_dataset_name: str = None,
    ) -> Sequence[DBTNodeResult]:
        """Tests `dbt` package

        Executes `dbt test` on previously cloned package.

        Args:
            run_params (Sequence[str], optional): Additional parameters to `test` command ie. test selectors`.
            additional_vars (StrAny, optional): Additional jinja variables to be passed to the package. Defaults to None.
            destination_dataset_name (str, optional): Overwrites the dbt schema where transformed models will be created. Useful for testing or creating several copies of transformed data . Defaults to None.

        Returns:
            Sequence[DBTNodeResult]: A list of executed tests with names, statuses, execution messages and execution times

        Exceptions:
            DBTProcessingError: `test` command failed. Contains a list of models with their execution statuses and error messages
        """
        return self._run_dbt_command(
            "test", cmd_params, self._get_package_vars(additional_vars, destination_dataset_name)
        )

    def _run_db_steps(
        self, run_params: Sequence[str], package_vars: StrAny, source_tests_selector: str
    ) -> Sequence[DBTNodeResult]:
        if self.repo_storage:
            # make sure we use package from the remote head
            self.ensure_newest_package()
        # run package deps
        self._run_dbt_command("deps")
        # run the tests on sources if specified, this prevents to execute package for which ie. the source schema does not yet exist
        try:
            if source_tests_selector:
                self.test(["-s", source_tests_selector], package_vars)
        except DBTProcessingError as err:
            raise PrerequisitesException(err) from err

        # always run seeds
        self._run_dbt_command("seed", package_vars=package_vars)

        # run package
        if run_params is None:
            run_params = []
        if not isinstance(run_params, list):
            # we always expect lists
            run_params = list(run_params)
        try:
            return self.run(run_params, package_vars)
        except IncrementalSchemaOutOfSyncError:
            if self.config.auto_full_refresh_when_out_of_sync:
                logger.warning("Attempting full refresh due to incremental model out of sync")
                return self.run(run_params + ["--full-refresh"], package_vars)
            else:
                raise

    def run_all(
        self,
        run_params: Sequence[str] = ("--fail-fast",),
        additional_vars: StrAny = None,
        source_tests_selector: str = None,
        destination_dataset_name: str = None,
    ) -> Sequence[DBTNodeResult]:
        """Prepares and runs a dbt package.

        This method executes typical `dbt` workflow with following steps:
        1. First it clones the package or brings it up to date with the origin. If package location is a local path, it stays intact
        2. It installs the dependencies (`dbt deps`)
        3. It runs seed (`dbt seed`)
        4. It runs optional tests on the sources
        5. It runs the package (`dbt run`)
        6. If the `dbt` fails with "incremental model out of sync", it will retry with full-refresh on (only when `auto_full_refresh_when_out_of_sync` is set).
           See https://docs.getdbt.com/docs/build/incremental-models#what-if-the-columns-of-my-incremental-model-change

        Args:
            run_params (Sequence[str], optional): Additional parameters to `run` command ie. `full-refresh`. Defaults to ("--fail-fast", ).
            additional_vars (StrAny, optional): Additional jinja variables to be passed to the package. Defaults to None.
            source_tests_selector (str, optional): A source tests selector ie. will execute all tests from `sources` model. Defaults to None.
            destination_dataset_name (str, optional): Overwrites the dbt schema where transformed models will be created. Useful for testing or creating several copies of transformed data . Defaults to None.

        Returns:
            Sequence[DBTNodeResult]: A list of processed model with names, statuses, execution messages and execution times

        Exceptions:
            DBTProcessingError: any of the dbt commands failed. Contains a list of models with their execution statuses and error messages
            PrerequisitesException: the source tests failed
            IncrementalSchemaOutOfSyncError: `run` failed due to schema being out of sync. the DBTProcessingError with failed model is in `args[0]`
        """
        try:
            results = self._run_db_steps(
                run_params,
                self._get_package_vars(additional_vars, destination_dataset_name),
                source_tests_selector,
            )
            self._log_dbt_run_results(results)
            return results
        except PrerequisitesException:
            logger.warning("Raw schema test failed, it may yet not be created")
            # run failed and loads possibly still pending
            raise
        except DBTProcessingError as runerr:
            self._log_dbt_run_results(runerr.run_results)
            # pass exception to the runner
            raise


@with_telemetry("helper", "dbt_create_runner", False, "package_profile_name")
@with_config(spec=DBTRunnerConfiguration, sections=(known_sections.DBT_PACKAGE_RUNNER,))
def create_runner(
    venv: Venv,
    credentials: DestinationClientDwhConfiguration,
    working_dir: str,
    package_location: str = dlt.config.value,
    package_repository_branch: str = None,
    package_repository_ssh_key: TSecretValue = TSecretValue(""),  # noqa
    package_profiles_dir: str = None,
    package_profile_name: str = None,
    auto_full_refresh_when_out_of_sync: bool = None,
    config: DBTRunnerConfiguration = None,
) -> DBTPackageRunner:
    """Creates a Python wrapper over `dbt` package present at specified location, that allows to control it (ie. run and test) from Python code.

    The created wrapper minimizes the required effort to run `dbt` packages. It clones the package repo and keeps it up to data,
    optionally shares the `dlt` destination credentials with `dbt` and allows the isolated execution with `venv` parameter.

    Note that you can pass config and secrets in DBTRunnerConfiguration as configuration in section "dbt_package_runner"

    Args:
        venv (Venv): A virtual environment with required dbt dependencies. Pass None to use current environment.
        credentials (DestinationClientDwhConfiguration): Any configuration deriving from DestinationClientDwhConfiguration ie. ConnectionStringCredentials
        working_dir (str): A working dir to which the package will be cloned
        package_location (str): A git repository url to be cloned or a local path where dbt package is present
        package_repository_branch (str, optional): A branch name, tag name or commit-id to check out. Defaults to None.
        package_repository_ssh_key (TSecretValue, optional): SSH key to be used to clone private repositories. Defaults to TSecretValue("").
        package_profiles_dir (str, optional): Path to the folder where "profiles.yml" resides
        package_profile_name (str, optional): Name of the profile in "profiles.yml"
        auto_full_refresh_when_out_of_sync (bool, optional): If set to True (default), the wrapper will automatically fall back to full-refresh mode when schema is out of sync
                                                             See: https://docs.getdbt.com/docs/build/incremental-models#what-if-the-columns-of-my-incremental-model-change_description_. Defaults to None.
        config (DBTRunnerConfiguration, optional): Explicit additional configuration for the runner.

    Returns:
        DBTPackageRunner: A Python `dbt` wrapper
    """
    dataset_name = credentials.dataset_name if credentials else ""
    if venv is None:
        venv = Venv.restore_current()
    return DBTPackageRunner(venv, credentials, working_dir, dataset_name, config)
