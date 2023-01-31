import os
from subprocess import CalledProcessError
import giturlparse
from typing import Any, ClassVar, Optional, Sequence
from prometheus_client import REGISTRY, Gauge, CollectorRegistry, Info
from prometheus_client.metrics import MetricWrapperBase

import dlt
from dlt.common import logger
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.configuration.utils import add_config_to_env
from dlt.common.runners.stdout import iter_stdout_with_result
from dlt.common.runners.venv import Venv
from dlt.common.typing import DictStrAny, DictStrStr, StrAny, TSecretValue
from dlt.common.logger import is_json_logging
from dlt.common.telemetry import get_logging_extras
from dlt.common.configuration import with_config
from dlt.common.storages import FileStorage
from dlt.common.git import git_custom_key_command, ensure_remote_head, clone_repo
from dlt.common.utils import with_custom_environ

from dlt.helpers.dbt.configuration import DBTRunnerConfiguration
from dlt.helpers.dbt.exceptions import IncrementalSchemaOutOfSyncError, PrerequisitesException, DBTNodeResult, DBTProcessingError


class DBTPackageRunner:
    """A Python wrapper over a dbt package

    The created wrapper minimizes the required effort to run `dbt` packages on datasets created with `dlt`. It clones the package repo and keeps it up to data,
    shares the `dlt` destination credentials with `dbt` and allows the isolated execution with `venv` parameter.
    The wrapper creates a `dbt` profile from a passed `dlt` credentials and executes the transformations in `source_dataset_name` schema. Additional configuration is
    passed via DBTRunnerConfiguration instance
    """

    model_elapsed_gauge: ClassVar[Gauge] = None
    model_exec_info: ClassVar[Info] = None

    def __init__(self,
        venv: Venv,
        credentials: CredentialsConfiguration,
        working_dir: str,
        source_dataset_name: str,
        config: DBTRunnerConfiguration,
        collector: CollectorRegistry = REGISTRY,
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
        try:
            self._create_gauges(collector)
        except ValueError as v:
            # ignore re-creation of gauges
            if "Duplicated" not in str(v):
                raise

    @staticmethod
    def _create_gauges(registry: CollectorRegistry) -> None:
        DBTPackageRunner.model_elapsed_gauge = Gauge("dbtrunner_model_elapsed_seconds", "Last model processing time", ["model"], registry=registry)
        DBTPackageRunner.model_exec_info = Info("dbtrunner_model_status", "Last execution status of the model", registry=registry)

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

    def _get_package_vars(self, additional_vars: StrAny = None, destination_dataset_name: str = None) -> StrAny:
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

        info: DictStrStr = {}
        for res in results:
            if res.status == "error":
                logger.error(f"Model {res.model_name} error! Error: {res.message}")
            else:
                logger.info(f"Model {res.model_name} {res.status} in {res.time} seconds with {res.message}")
            DBTPackageRunner.model_elapsed_gauge.labels(res.model_name).set(res.time)
            info[res.model_name] = res.message

        # log execution
        DBTPackageRunner.model_exec_info.info(info)
        logger.metrics("stop", "dbt models", extra=get_logging_extras([DBTPackageRunner.model_elapsed_gauge, DBTPackageRunner.model_exec_info]))

    def _clone_package(self, with_git_command: Optional[str]) -> None:
        try:
            # cleanup package folder
            if self.repo_storage.has_folder(self.cloned_package_name):
                self.repo_storage.delete_folder(self.cloned_package_name, recursively=True, delete_ro=True)
            logger.info(f"Will clone {self.config.package_location} head {self.config.package_repository_branch} into {self.package_path}")
            clone_repo(self.config.package_location, self.package_path, branch=self.config.package_repository_branch, with_git_command=with_git_command).close()

        except Exception:
            # delete folder so we start clean next time
            if self.repo_storage.has_folder(self.cloned_package_name):
                self.repo_storage.delete_folder(self.cloned_package_name, recursively=True, delete_ro=True)
            raise

    def ensure_newest_package(self) -> None:
        """Clones or brings the dbt package at `package_location` up to date."""
        from git import GitError

        with git_custom_key_command(self.config.package_repository_ssh_key) as ssh_command:
            try:
                ensure_remote_head(self.package_path, with_git_command=ssh_command)
            except GitError as err:
                # cleanup package folder
                logger.info(f"Package will be cloned due to {type(err).__name__}:{str(err)}")
                self._clone_package(with_git_command=ssh_command)

    @with_custom_environ
    def _run_dbt_command(self, command: str, command_args: Sequence[str] = None, package_vars: StrAny = None) -> Sequence[DBTNodeResult]:
        logger.info(f"Exec dbt command: {command} {command_args} {package_vars} on profile {self.config.package_profile_name}")
        # write credentials to environ to pass them to dbt
        if self.credentials:
            add_config_to_env(self.credentials)
        args = [
            self.config.runtime.log_level,
            is_json_logging(self.config.runtime.log_format),
            self.package_path,
            command,
            self.config.package_profiles_dir,
            self.config.package_profile_name,
            command_args,
            package_vars
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

    def run(self, cmd_params: Sequence[str] = ("--fail-fast", ), additional_vars: StrAny = None, destination_dataset_name: str = None) -> Sequence[DBTNodeResult]:
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
            "run",
            cmd_params,
            self._get_package_vars(additional_vars, destination_dataset_name)
        )

    def test(self, cmd_params: Sequence[str] = None, additional_vars: StrAny = None, destination_dataset_name: str = None) -> Sequence[DBTNodeResult]:
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
            "test",
            cmd_params,
            self._get_package_vars(additional_vars, destination_dataset_name)
        )

    def _run_db_steps(self, run_params: Sequence[str], package_vars: StrAny, source_tests_selector: str) -> Sequence[DBTNodeResult]:
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

    def run_all(self,
        run_params: Sequence[str] = ("--fail-fast", ),
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
                source_tests_selector
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


@with_config(spec=DBTRunnerConfiguration, namespaces=("dbt_package_runner",))
def create_runner(
    venv: Venv,
    credentials: CredentialsConfiguration,
    working_dir: str,
    dataset_name: str,
    package_location: str = dlt.config.value,
    package_repository_branch: str = None,
    package_repository_ssh_key: TSecretValue = TSecretValue(""),  # noqa
    package_profiles_dir: str = None,
    package_profile_name: str = None,
    auto_full_refresh_when_out_of_sync: bool = None,
    config: DBTRunnerConfiguration = None
    ) -> DBTPackageRunner:
    return DBTPackageRunner(venv, credentials, working_dir, dataset_name, config, REGISTRY)
