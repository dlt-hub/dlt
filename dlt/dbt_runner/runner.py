from typing import Optional, Sequence, Tuple
from prometheus_client import REGISTRY, Gauge, CollectorRegistry, Info
from prometheus_client.metrics import MetricWrapperBase

from dlt.common import logger
from dlt.cli import TRunnerArgs
from dlt.common.typing import DictStrAny, DictStrStr, StrAny
from dlt.common.logger import is_json_logging
from dlt.common.telemetry import get_logging_extras
from dlt.common.configuration.specs import GcpClientCredentials
from dlt.common.storages import FileStorage
from dlt.common.runners import initialize_runner, run_pool
from dlt.common.telemetry import TRunMetrics
from dlt.common.git import git_custom_key_command, ensure_remote_head, clone_repo

from dlt.dbt_runner.configuration import DBTRunnerConfiguration, gen_configuration_variant
from dlt.dbt_runner.utils import DBTProcessingError, dbt_results, initialize_dbt_logging, is_incremental_schema_out_of_sync_error, run_dbt_command
from dlt.dbt_runner.exceptions import PrerequisitesException


CLONED_PACKAGE_NAME = "dbt_package"

CONFIG: DBTRunnerConfiguration = None
storage: FileStorage = None
dbt_package_vars: StrAny = None
global_args: Sequence[str] = None
repo_path: str = None
profile_name: str = None

model_elapsed_gauge: Gauge = None
model_exec_info: Info = None


def create_folders() -> Tuple[FileStorage, StrAny, Sequence[str], str, str]:
    storage_ = FileStorage(CONFIG.package_volume_path, makedirs=True)
    dbt_package_vars_: DictStrAny = {
        "source_schema_prefix": CONFIG.source_schema_prefix
    }
    if CONFIG.dest_schema_prefix:
        dbt_package_vars_["dest_schema_prefix"] = CONFIG.dest_schema_prefix
    if CONFIG.package_additional_vars:
        dbt_package_vars_.update(CONFIG.package_additional_vars)

    # initialize dbt logging, returns global parameters to dbt command
    global_args_ = initialize_dbt_logging(CONFIG.log_level, is_json_logging(CONFIG.log_format))

    # generate path for the dbt package repo
    repo_path_ = storage_.make_full_path(CLONED_PACKAGE_NAME)

    # generate profile name
    profile_name_: str = None
    if CONFIG.package_profile_prefix:
        if isinstance(CONFIG, GcpClientCredentials):
            profile_name_ = "%s_bigquery" % (CONFIG.package_profile_prefix)
        else:
            profile_name_ = "%s_redshift" % (CONFIG.package_profile_prefix)

    return storage_, dbt_package_vars_, global_args_, repo_path_, profile_name_


def create_gauges(registry: CollectorRegistry) -> Tuple[MetricWrapperBase, MetricWrapperBase]:
    return (
        Gauge("dbtrunner_model_elapsed_seconds", "Last model processing time", ["model"], registry=registry),
        Info("dbtrunner_model_status", "Last execution status of the model", registry=registry)
    )


def run_dbt(command: str, command_args: Sequence[str] = None) -> Sequence[dbt_results. BaseResult]:
    logger.info(f"Exec dbt command: {global_args} {command} {command_args} {dbt_package_vars} on profile {profile_name or '<project_default>'}")
    return run_dbt_command(
        repo_path, command,
        CONFIG.package_profiles_dir,
        profile_name=profile_name,
        command_args=command_args,
        global_args=global_args,
        dbt_vars=dbt_package_vars
    )


def log_dbt_run_results(results: dbt_results.RunExecutionResult) -> None:
    # run may return RunResult of something different depending on error
    if issubclass(type(results), dbt_results.BaseResult):
        results = [results]  # make it iterable
    elif issubclass(type(results), dbt_results.ExecutionResult):
        pass
    else:
        logger.warning(f"{type(results)} is unknown and cannot be logged")
        return

    info: DictStrStr = {}
    for res in results:
        name = res.node.name
        message = res.message
        time = res.execution_time
        if res.status == dbt_results.RunStatus.Error:
            logger.error(f"Model {name} error! Error: {message}")
        else:
            logger.info(f"Model {name} {res.status} in {time} seconds with {message}")
        model_elapsed_gauge.labels(name).set(time)
        info[name] = message

    # log execution
    model_exec_info.info(info)
    logger.metrics("stop", "dbt models", extra=get_logging_extras([model_elapsed_gauge, model_exec_info]))


def initialize_package(with_git_command: Optional[str]) -> None:
    try:
        # cleanup package folder
        if storage.has_folder(CLONED_PACKAGE_NAME):
            storage.delete_folder(CLONED_PACKAGE_NAME, recursively=True)
        logger.info(f"Will clone {CONFIG.package_repository_url} head {CONFIG.package_repository_branch} into {repo_path}")
        clone_repo(CONFIG.package_repository_url, repo_path, branch=CONFIG.package_repository_branch, with_git_command=with_git_command)
        run_dbt("deps")
    except Exception:
        # delete folder so we start clean next time
        if storage.has_folder(CLONED_PACKAGE_NAME):
            storage.delete_folder(CLONED_PACKAGE_NAME, recursively=True)
        raise


def ensure_newest_package() -> None:
    from git import GitError

    with git_custom_key_command(CONFIG.package_repository_ssh_key) as ssh_command:
        try:
            ensure_remote_head(repo_path, with_git_command=ssh_command)
        except GitError as err:
            # cleanup package folder
            logger.info(f"Package will be cloned due to {type(err).__name__}:{str(err)}")
            initialize_package(with_git_command=ssh_command)


def run_db_steps() -> Sequence[dbt_results.BaseResult]:
    # make sure we use package from the remote head
    ensure_newest_package()
    # check if raw schema exists
    try:
        if CONFIG.package_source_tests_selector:
            run_dbt("test", ["-s", CONFIG.package_source_tests_selector])
    except DBTProcessingError as err:
        raise PrerequisitesException() from err

    # always run seeds
    run_dbt("seed")
    # throws DBTProcessingError
    try:
        return run_dbt("run", CONFIG.package_run_params)
    except DBTProcessingError as e:
        # detect incremental model out of sync
        if is_incremental_schema_out_of_sync_error(e.results) and CONFIG.auto_full_refresh_when_out_of_sync:
            logger.warning(f"Attempting full refresh due to incremental model out of sync on {e.results.message}")
            return run_dbt("run", CONFIG.package_run_params + ["--full-refresh"])
        else:
            raise


def run(_: None) -> TRunMetrics:
    try:
        # there were many issues with running the method below with pool.apply
        # 1 - some exceptions are not serialized well on process boundary and queue hangs
        # 2 - random hangs event if there's no exception, probably issues with DBT spawning its own workers
        # instead the runner host was configured to recycle each run
        results = run_db_steps()
        log_dbt_run_results(results)
        return TRunMetrics(False, False, 0)
    except PrerequisitesException:
        logger.warning("Raw schema test failed, it may yet not be created")
        # run failed and loads possibly still pending
        return TRunMetrics(False, True, 1)
    except DBTProcessingError as runerr:
        log_dbt_run_results(runerr.results)
        # pass exception to the runner
        raise


def configure(C: DBTRunnerConfiguration, collector: CollectorRegistry) -> None:
    global CONFIG
    global storage, dbt_package_vars, global_args, repo_path, profile_name
    global model_elapsed_gauge, model_exec_info

    CONFIG = C
    storage, dbt_package_vars, global_args, repo_path, profile_name = create_folders()
    try:
        model_elapsed_gauge, model_exec_info = create_gauges(REGISTRY)
    except ValueError as v:
        # ignore re-creation of gauges
        if "Duplicated" not in str(v):
            raise


def main(args: TRunnerArgs) -> int:
    C = gen_configuration_variant(args._asdict())
    # we should force single run
    initialize_runner(C)
    try:
        configure(C, REGISTRY)
    except Exception:
        logger.exception("init module")
        return -1

    return run_pool(C, run)


def run_main(args: TRunnerArgs) -> None:
    exit(main(args))
