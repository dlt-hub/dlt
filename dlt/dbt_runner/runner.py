import os
import giturlparse
from typing import Any, ClassVar, Optional, Sequence, cast
from prometheus_client import REGISTRY, Gauge, CollectorRegistry, Info
from prometheus_client.metrics import MetricWrapperBase

import dlt
from dlt.common import logger
from dlt.common.configuration.inject import last_config
from dlt.common.typing import DictStrAny, DictStrStr
from dlt.common.logger import is_json_logging
from dlt.common.telemetry import get_logging_extras
from dlt.common.configuration import with_config
from dlt.common.storages import FileStorage
from dlt.common.git import git_custom_key_command, ensure_remote_head, clone_repo

from dlt.dbt_runner.configuration import DBTRunnerConfiguration
from dlt.dbt_runner.dbt_utils import dbt_results, initialize_dbt_logging, is_incremental_schema_out_of_sync_error, parse_dbt_execution_results, run_dbt_command
from dlt.dbt_runner.exceptions import PrerequisitesException, DBTNodeResult, DBTProcessingError


class DBTRunner:

    model_elapsed_gauge: ClassVar[Gauge] = None
    model_exec_info: ClassVar[Info] = None

    def __init__(self,
        working_dir: str,
        source_dataset_name: str,
        profile_name: str,
        config: DBTRunnerConfiguration,
        collector: CollectorRegistry = REGISTRY,
    ) -> None:
        self.working_dir = working_dir
        self.source_dataset_name = source_dataset_name
        self.config = config
        self.profile_name = profile_name

        self.package_vars: DictStrAny = None
        self.dbt_global_args: Sequence[str] = None
        self.package_path: str = None
        # set if package is in repo
        self.repo_storage: FileStorage = None
        self.cloned_package_name: str = None

        self.setup_package()
        try:
            self.create_gauges(collector)
        except ValueError as v:
            # ignore re-creation of gauges
            if "Duplicated" not in str(v):
                raise

    @staticmethod
    def create_gauges(registry: CollectorRegistry) -> None:
        DBTRunner.model_elapsed_gauge = Gauge("dbtrunner_model_elapsed_seconds", "Last model processing time", ["model"], registry=registry)
        DBTRunner.model_exec_info = Info("dbtrunner_model_status", "Last execution status of the model", registry=registry)

    def setup_package(self) -> None:
        self.package_vars = {
            "source_dataset_name": self.source_dataset_name
        }
        if self.config.destination_dataset_name:
            self.package_vars["destination_dataset_name"] = self.config.destination_dataset_name
        if self.config.package_additional_vars:
            self.package_vars.update(self.config.package_additional_vars)

        # initialize dbt logging, returns global parameters to dbt command
        self.dbt_global_args = initialize_dbt_logging(self.config.runtime.log_level, is_json_logging(self.config.runtime.log_format))

        # set the package location
        url = giturlparse.parse(self.config.package_location, check_domain=False)
        if not url.valid:
            self.package_path = self.config.package_location
        else:
            # location is a repository
            self.repo_storage = FileStorage(self.working_dir, makedirs=True)
            self.cloned_package_name = url.name
            self.package_path = os.path.join(self.working_dir, self.cloned_package_name)

    def run_dbt(self, command: str, command_args: Sequence[str] = None) -> dbt_results.ExecutionResult:
        logger.info(f"Exec dbt command: {self.dbt_global_args} {command} {command_args} {self.package_vars} on profile {self.profile_name}")
        # run dbt with modified environ
        return run_dbt_command(
            self.package_path,
            command,
            self.config.package_profiles_dir,
            profile_name=self.profile_name,
            command_args=command_args,
            global_args=self.dbt_global_args,
            dbt_vars=self.package_vars
        )

    def log_dbt_run_results(self, results: Sequence[DBTNodeResult]) -> None:
        if not results:
            return

        info: DictStrStr = {}
        for res in results:
            if res.status == dbt_results.RunStatus.Error:
                logger.error(f"Model {res.model_name} error! Error: {res.message}")
            else:
                logger.info(f"Model {res.model_name} {res.status} in {res.time} seconds with {res.message}")
            DBTRunner.model_elapsed_gauge.labels(res.model_name).set(res.time)
            info[res.model_name] = res.message

        # log execution
        DBTRunner.model_exec_info.info(info)
        logger.metrics("stop", "dbt models", extra=get_logging_extras([DBTRunner.model_elapsed_gauge, DBTRunner.model_exec_info]))

    def clone_package(self, with_git_command: Optional[str]) -> None:
        try:
            # cleanup package folder
            if self.repo_storage.has_folder(self.cloned_package_name):
                self.repo_storage.delete_folder(self.cloned_package_name, recursively=True)
            logger.info(f"Will clone {self.config.package_location} head {self.config.package_repository_branch} into {self.package_path}")
            clone_repo(self.config.package_location, self.package_path, branch=self.config.package_repository_branch, with_git_command=with_git_command)

        except Exception:
            # delete folder so we start clean next time
            if self.repo_storage.has_folder(self.cloned_package_name):
                self.repo_storage.delete_folder(self.cloned_package_name, recursively=True)
            raise

    def ensure_newest_package(self) -> None:
        from git import GitError

        with git_custom_key_command(self.config.package_repository_ssh_key) as ssh_command:
            try:
                ensure_remote_head(self.package_path, with_git_command=ssh_command)
            except GitError as err:
                # cleanup package folder
                logger.info(f"Package will be cloned due to {type(err).__name__}:{str(err)}")
                self.clone_package(with_git_command=ssh_command)

    def run_db_steps(self) -> Sequence[DBTNodeResult]:
        if self.repo_storage:
            # make sure we use package from the remote head
            self.ensure_newest_package()
        # run package deps
        self.run_dbt("deps")
        # check if raw schema exists
        try:
            if self.config.package_source_tests_selector:
                self.run_dbt("test", ["-s", self.config.package_source_tests_selector])
        except DBTProcessingError as err:
            raise PrerequisitesException(err) from err

        # always run seeds
        self.run_dbt("seed")
        # throws DBTProcessingError
        try:
            return parse_dbt_execution_results(
                    self.run_dbt("run", self.config.package_run_params)
                )
        except DBTProcessingError as e:
            # detect incremental model out of sync
            if is_incremental_schema_out_of_sync_error(e.dbt_results) and self.config.auto_full_refresh_when_out_of_sync:
                logger.warning("Attempting full refresh due to incremental model out of sync")
                return parse_dbt_execution_results(
                        self.run_dbt("run", list(self.config.package_run_params) + ["--full-refresh"])
                    )
            else:
                raise

    def run(self) -> Sequence[DBTNodeResult]:
        # TODO: pass destination dataset, package_run_params, additional vars here, not via config file
        try:
            # there were many issues with running the method below with pool.apply
            # 1 - some exceptions are not serialized well on process boundary and queue hangs
            # 2 - random hangs event if there's no exception, probably issues with DBT spawning its own workers
            # instead the runner host was configured to recycle each run
            results = self.run_db_steps()
            self.log_dbt_run_results(results)
            return results
        except PrerequisitesException:
            logger.warning("Raw schema test failed, it may yet not be created")
            # run failed and loads possibly still pending
            raise
        except DBTProcessingError as runerr:
            self.log_dbt_run_results(runerr.run_results)
            # pass exception to the runner
            raise


@with_config(spec=DBTRunnerConfiguration, namespaces=("dbt_runner",))
def get_runner(
    working_dir: str,
    default_profile_name: str,
    dataset_name: str,
    package_location: str = dlt.config.value,
    package_run_params: Sequence[str] = None,
    package_source_tests_selector: str = None,
    destination_dataset_name: str = None,
    config: DBTRunnerConfiguration = None,
    **kwargs: Any
    ) -> DBTRunner:
    # config: DBTRunnerConfiguration = last_config(**kwargs)
    return DBTRunner(working_dir, dataset_name, config.package_profile_name or default_profile_name, config, REGISTRY)
