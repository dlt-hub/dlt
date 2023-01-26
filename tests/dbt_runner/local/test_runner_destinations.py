import os
from typing import Any
from git import GitCommandError
import pytest
from dlt.common.telemetry import get_metrics_from_prometheus

from dlt.common.typing import StrStr
from dlt.common.utils import uniq_id

from dlt.dbt_runner.dbt_utils import DBTProcessingError
from dlt.dbt_runner.exceptions import PrerequisitesException
from tests.dbt_runner.utils import find_run_result

from tests.utils import TEST_STORAGE_ROOT, clean_test_storage, preserve_environ
from tests.common.utils import modify_and_commit_file, load_secret
from tests.dbt_runner.local.utils import setup_rasa_runner_client, setup_rasa_runner, DBTDestinationInfo

DESTINATION_DATASET_NAME = "test_" + uniq_id()
ALL_DBT_DESTINATIONS = [DBTDestinationInfo("redshift", "SELECT", "INSERT"), DBTDestinationInfo("bigquery", "CREATE TABLE", "MERGE")]
ALL_DBT_DESTINATIONS_NAMES = ["redshift", "bigquery"]


@pytest.fixture(scope="module", params=ALL_DBT_DESTINATIONS, ids=ALL_DBT_DESTINATIONS_NAMES)
def destination_info(request: Any) -> DBTDestinationInfo:
    # this resolves credentials and sets up env for dbt then deletes temp datasets
    with setup_rasa_runner_client(request.param.destination_name, DESTINATION_DATASET_NAME):
        # yield DBTDestinationInfo
        yield request.param


def test_setup_dbt_runner() -> None:
    runner = setup_rasa_runner("eks_dev_dest", "redshift", "carbon_bot_3", override_values={
        "package_additional_vars": {"add_var_name": "add_var_value"},
        "runtime": {
            "log_format": "JSON",
            "log_level": "INFO"
        }
    })
    assert runner.package_path.endswith("rasa_semantic_schema")
    assert runner.profile_name == "redshift"
    assert runner.dbt_global_args == ["--log-format", "json"]
    assert runner.package_vars == {"source_dataset_name": "carbon_bot_3", "destination_dataset_name": "eks_dev_dest", "add_var_name": "add_var_value"}
    assert runner.source_dataset_name == "carbon_bot_3"
    assert runner.cloned_package_name == "rasa_semantic_schema"
    assert runner.working_dir == TEST_STORAGE_ROOT


def test_initialize_package_wrong_key() -> None:
    runner = setup_rasa_runner(DESTINATION_DATASET_NAME, "redshift", override_values={
        # private repo
        "package_location": "git@github.com:scale-vector/rasa_bot_experiments.git"
    })
    runner.config.package_repository_ssh_key = load_secret("DEPLOY_KEY")
    with pytest.raises(GitCommandError) as gce:
        runner.run()
    assert "Could not read from remote repository" in gce.value.stderr


def test_reinitialize_package() -> None:
    runner = setup_rasa_runner(DESTINATION_DATASET_NAME, "redshift")
    runner.ensure_newest_package()
    # mod the package
    readme_path = modify_and_commit_file(runner.package_path, "README.md", content=runner.config.destination_dataset_name)
    assert os.path.isfile(readme_path)
    # this will wipe out old package and clone again
    runner.ensure_newest_package()
    # we have old file back
    assert runner.repo_storage.load(f"{runner.cloned_package_name}/README.md") != runner.config.destination_dataset_name


def test_dbt_test_no_raw_schema(destination_info: DBTDestinationInfo) -> None:
    # force non existing dataset
    runner = setup_rasa_runner(DESTINATION_DATASET_NAME, destination_info.destination_name, "jm_dev_2" + uniq_id())
    # source test should not pass
    with pytest.raises(PrerequisitesException) as prq_ex:
        runner.run()
    assert isinstance(prq_ex.value.args[0], DBTProcessingError)


def test_dbt_run_full_refresh(destination_info: DBTDestinationInfo) -> None:
    runner = setup_rasa_runner(DESTINATION_DATASET_NAME, destination_info.destination_name, override_values={
            "package_additional_vars": {"user_id": "metadata__user_id"}
        })
    run_results = runner.run()
    assert all(r.message.startswith(destination_info.replace_strategy) for r in run_results) is True
    assert find_run_result(run_results, "_loads") is not None

    # do the same checks using the metrics
    metrics: StrStr = get_metrics_from_prometheus([runner.model_exec_info])["dbtrunner_model_status_info"]
    # all models must be SELECT as we do full refresh
    assert all(v.startswith(destination_info.replace_strategy) for k,v in metrics.items()) is True
    assert metrics["_loads"].startswith(destination_info.replace_strategy)
    # all tests should pass
    runner.run_dbt("test")


def test_dbt_run_error_via_additional_vars(destination_info: DBTDestinationInfo) -> None:
    # generate with setting external user and session to non existing fields (metadata__sess_id not exists in JM schema)
    runner = setup_rasa_runner(DESTINATION_DATASET_NAME, destination_info.destination_name, override_values={
        "package_additional_vars": {"user_id": "metadata__user_id", "external_session_id": "metadata__sess_id"}
    })
    with pytest.raises(DBTProcessingError) as dbt_err:
        runner.run()
    stg_interactions = find_run_result(dbt_err.value.run_results, "stg_interactions")
    assert "metadata__sess_id" in stg_interactions.message

    # same check with metrics
    metrics: StrStr = get_metrics_from_prometheus([runner.model_exec_info])["dbtrunner_model_status_info"]
    assert "stg_interactions" in metrics
    assert "metadata__sess_id" in metrics["stg_interactions"]


def test_dbt_incremental_schema_out_of_sync_error(destination_info: DBTDestinationInfo) -> None:
    runner = setup_rasa_runner(DESTINATION_DATASET_NAME, destination_info.destination_name, override_values={
        # run stg_interactions and all parents
        "package_run_params": ["--fail-fast", "--model", "+interactions"],
        # remove all counter metrics
        "package_additional_vars": {"count_metrics": []}
    })
    runner.run()

    runner = setup_rasa_runner(DESTINATION_DATASET_NAME, destination_info.destination_name, override_values={
        # run stg_interactions and all parents
        "package_run_params": ["--fail-fast", "--model", "+interactions"],
        # allow count metrics to generate schema error
        "package_additional_vars": {}
    })
    runner.run()
    metrics: StrStr = get_metrics_from_prometheus([runner.model_exec_info])["dbtrunner_model_status_info"]
    # full refresh on interactions
    assert metrics["interactions"].startswith(destination_info.replace_strategy)

    # now incremental load should happen
    runner.run()
    metrics: StrStr = get_metrics_from_prometheus([runner.model_exec_info])["dbtrunner_model_status_info"]
    # full refresh on interactions
    assert metrics["interactions"].startswith(destination_info.incremental_strategy)
