import os
import shutil
import pytest
from dlt.common.runners.synth_pickle import decode_obj

from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id
from dlt.dbt_runner.dbt_utils import DBTProcessingError, initialize_dbt_logging, run_dbt_command, dbt_results, parse_dbt_execution_results, is_incremental_schema_out_of_sync_error
from tests.utils import test_storage
from tests.dbt_runner.utils import clone_jaffle_repo, load_test_case


def test_is_incremental_schema_out_of_sync_error() -> None:
    # in case of --fail-fast detect on a single run result
    assert is_incremental_schema_out_of_sync_error(decode_obj(load_test_case("run_result_incremental_fail.pickle.hex"))) is True
    assert is_incremental_schema_out_of_sync_error(decode_obj(load_test_case("run_execution_incremental_fail.pickle.hex"))) is True
    assert is_incremental_schema_out_of_sync_error("AAA") is False


def test_dbt_commands(test_storage: FileStorage) -> None:
    schema_name = "s_" + uniq_id()
    # profiles in cases require this var to be set
    dbt_vars = {"dbt_schema": schema_name}

    repo_path = clone_jaffle_repo(test_storage)
    # copy profile
    shutil.copy("./tests/dbt_runner/cases/profiles_invalid_credentials.yml", os.path.join(repo_path, "profiles.yml"))
    # initialize logging
    global_args = initialize_dbt_logging("INFO", False)
    # run deps, results are None
    assert run_dbt_command(repo_path, "deps", ".", global_args=global_args) is None

    # run list, results are list of strings
    results = run_dbt_command(repo_path, "list", ".", global_args=global_args, dbt_vars=dbt_vars)
    assert isinstance(results, list)
    assert len(results) == 28
    assert "jaffle_shop.not_null_orders_amount" in results
    # run list for specific selector
    results = run_dbt_command(repo_path, "list", ".", global_args=global_args, command_args=["-s", "jaffle_shop.not_null_orders_amount"], dbt_vars=dbt_vars)
    assert len(results) == 1
    assert results[0] == "jaffle_shop.not_null_orders_amount"
    # run debug, that will fail
    with pytest.raises(DBTProcessingError) as dbt_err:
        run_dbt_command(repo_path, "debug", ".", global_args=global_args, dbt_vars=dbt_vars)
    # results are bool
    assert dbt_err.value.command == "debug"

    # we have no database connectivity so tests will fail
    with pytest.raises(DBTProcessingError) as dbt_err:
        run_dbt_command(repo_path, "test", ".", global_args=global_args, dbt_vars=dbt_vars)
    # in that case test results are bool, not list of tests runs
    assert dbt_err.value.command == "test"

    # same for run
    with pytest.raises(DBTProcessingError) as dbt_err:
        run_dbt_command(repo_path, "run", ".", global_args=global_args, dbt_vars=dbt_vars, command_args=["--fail-fast", "--full-refresh"])
    # in that case test results are bool, not list of tests runs
    assert dbt_err.value.command == "run"

    # copy a correct profile
    shutil.copy("./tests/dbt_runner/cases/profiles.yml", os.path.join(repo_path, "profiles.yml"))

    seed_info = run_dbt_command(repo_path, "seed", ".", global_args=global_args, dbt_vars=dbt_vars)
    assert isinstance(seed_info, dbt_results.RunExecutionResult)
    results = parse_dbt_execution_results(seed_info)
    assert len(results) == 3
    assert results[0].model_name == "raw_customers"
    assert results[0].status == "success"

    run_info = run_dbt_command(repo_path, "run", ".", global_args=global_args, dbt_vars=dbt_vars, command_args=["--fail-fast", "--full-refresh"])
    assert isinstance(run_info, dbt_results.RunExecutionResult)
    results = parse_dbt_execution_results(run_info)
    assert len(results) == 5
    assert results[-1].model_name == "orders"
    assert results[-1].status == "success"

    test_info = run_dbt_command(repo_path, "test", ".", global_args=global_args, dbt_vars=dbt_vars)
    assert isinstance(test_info, dbt_results.RunExecutionResult)
    results = parse_dbt_execution_results(test_info)
    assert len(results) == 20
    assert results[-1].status == "pass"
