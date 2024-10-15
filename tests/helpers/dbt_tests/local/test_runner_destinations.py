import os
from typing import Any, Iterator
from git import GitCommandError
import pytest

from dlt.common.utils import uniq_id

from dlt.helpers.dbt.dbt_utils import DBTProcessingError
from dlt.helpers.dbt.exceptions import PrerequisitesException
from tests.helpers.dbt_tests.utils import find_run_result

from tests.utils import TEST_STORAGE_ROOT, clean_test_storage, preserve_environ
from tests.common.utils import modify_and_commit_file, load_secret
from tests.helpers.dbt_tests.local.utils import (
    setup_rasa_runner_client,
    setup_rasa_runner,
    DBTDestinationInfo,
)

DESTINATION_DATASET_NAME = "test_" + uniq_id()
ALL_DBT_DESTINATIONS = [
    DBTDestinationInfo("bigquery", "CREATE TABLE", "MERGE")
]  # DBTDestinationInfo("redshift", "SELECT", "INSERT")
ALL_DBT_DESTINATIONS_NAMES = ["bigquery"]  # "redshift",


@pytest.fixture(scope="module", params=ALL_DBT_DESTINATIONS, ids=ALL_DBT_DESTINATIONS_NAMES)
def destination_info(request: Any) -> Iterator[DBTDestinationInfo]:
    # this resolves credentials and sets up env for dbt then deletes temp datasets
    with setup_rasa_runner_client(request.param.destination_name, DESTINATION_DATASET_NAME):
        # yield DBTDestinationInfo
        yield request.param


def test_setup_dbt_runner() -> None:
    runner = setup_rasa_runner(
        "redshift",
        "carbon_bot_3",
        override_values={
            "package_additional_vars": {"add_var_name": "add_var_value"},
            "runtime": {"log_format": "JSON", "log_level": "INFO"},
        },
    )
    assert runner.package_path.endswith("rasa_semantic_schema")
    assert runner.config.package_profile_name == "redshift"
    assert runner.config.package_additional_vars == {"add_var_name": "add_var_value"}
    assert runner._get_package_vars() == {
        "source_dataset_name": "carbon_bot_3",
        "add_var_name": "add_var_value",
    }
    assert runner.source_dataset_name == "carbon_bot_3"
    assert runner.cloned_package_name == "rasa_semantic_schema"
    assert runner.working_dir == TEST_STORAGE_ROOT


def test_initialize_package_wrong_key() -> None:
    runner = setup_rasa_runner(
        "redshift",
        override_values={
            # private repo
            "package_location": "git@github.com:dlt-hub/rasa_bot_experiments.git",
            "package_repository_branch": None,
            "package_repository_ssh_key": load_secret("DEPLOY_KEY"),
        },
    )

    with pytest.raises(GitCommandError) as gce:
        runner.run_all()
    assert "Could not read from remote repository" in gce.value.stderr


def test_reinitialize_package() -> None:
    runner = setup_rasa_runner("redshift")
    runner.ensure_newest_package()
    # mod the package
    readme_path, _ = modify_and_commit_file(
        runner.package_path, "README.md", content=runner.config.package_profiles_dir
    )
    assert os.path.isfile(readme_path)
    # this will wipe out old package and clone again
    runner.ensure_newest_package()
    # we have old file back
    assert (
        runner.repo_storage.load(f"{runner.cloned_package_name}/README.md")
        != runner.config.package_profiles_dir
    )


def test_dbt_test_no_raw_schema(destination_info: DBTDestinationInfo) -> None:
    # force non existing dataset
    runner = setup_rasa_runner(destination_info.destination_name, "jm_dev_2" + uniq_id())
    # source test should not pass
    with pytest.raises(PrerequisitesException) as prq_ex:
        runner.run_all(
            destination_dataset_name=DESTINATION_DATASET_NAME,
            run_params=["--fail-fast", "--full-refresh"],
            source_tests_selector="tag:prerequisites",
        )
    assert isinstance(prq_ex.value.args[0], DBTProcessingError)


def test_dbt_run_dev_mode(destination_info: DBTDestinationInfo) -> None:
    if destination_info.destination_name == "redshift":
        pytest.skip("redshift disabled due to missing fixtures")
    runner = setup_rasa_runner(destination_info.destination_name)
    run_results = runner.run_all(
        destination_dataset_name=DESTINATION_DATASET_NAME,
        run_params=["--fail-fast", "--full-refresh"],
        additional_vars={"user_id": "metadata__user_id"},
        source_tests_selector="tag:prerequisites",
    )
    assert all(r.message.startswith(destination_info.replace_strategy) for r in run_results) is True
    assert find_run_result(run_results, "_loads") is not None
    # all models must be SELECT as we do full refresh
    assert find_run_result(run_results, "_loads").message.startswith(
        destination_info.replace_strategy
    )
    assert all(m.message.startswith(destination_info.replace_strategy) for m in run_results) is True

    # all tests should pass
    runner.test(
        destination_dataset_name=DESTINATION_DATASET_NAME,
        additional_vars={"user_id": "metadata__user_id"},
    )


def test_dbt_run_error_via_additional_vars(destination_info: DBTDestinationInfo) -> None:
    if destination_info.destination_name == "redshift":
        pytest.skip("redshift disabled due to missing fixtures")
    # generate with setting external user and session to non existing fields (metadata__sess_id not exists in JM schema)
    runner = setup_rasa_runner(destination_info.destination_name)
    with pytest.raises(DBTProcessingError) as dbt_err:
        runner.run_all(
            destination_dataset_name=DESTINATION_DATASET_NAME,
            run_params=["--fail-fast", "--full-refresh"],
            additional_vars={
                "user_id": "metadata__user_id",
                "external_session_id": "metadata__sess_id",
            },
            source_tests_selector="tag:prerequisites",
        )
    stg_interactions = find_run_result(dbt_err.value.run_results, "stg_interactions")
    assert "metadata__sess_id" in stg_interactions.message


def test_dbt_incremental_schema_out_of_sync_error(destination_info: DBTDestinationInfo) -> None:
    if destination_info.destination_name == "redshift":
        pytest.skip("redshift disabled due to missing fixtures")
    runner = setup_rasa_runner(destination_info.destination_name)
    runner.run_all(
        destination_dataset_name=DESTINATION_DATASET_NAME,
        # run stg_interactions and all parents
        run_params=["--fail-fast", "--model", "+interactions"],
        # remove all counter metrics
        additional_vars={"count_metrics": []},
        source_tests_selector="tag:prerequisites",
    )

    # generate schema error on incremental load
    results = runner.run_all(
        destination_dataset_name=DESTINATION_DATASET_NAME,
        # run stg_interactions and all parents
        run_params=["--fail-fast", "--model", "+interactions"],
        # allow count metrics to generate schema error
        additional_vars={},
    )
    # full refresh on interactions
    assert find_run_result(results, "interactions").message.startswith(
        destination_info.replace_strategy
    )

    # now incremental load should happen
    results = runner.run(
        ["--fail-fast", "--model", "+interactions"],
        destination_dataset_name=DESTINATION_DATASET_NAME,
        additional_vars={},
    )
    interactions = find_run_result(results, "interactions")
    # incremental on interactions
    assert interactions.message.startswith(destination_info.incremental_strategy)
