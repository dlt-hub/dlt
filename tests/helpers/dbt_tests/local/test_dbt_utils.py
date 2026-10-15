import os
import shutil
from datetime import datetime, timedelta, timezone
from typing import Any, Tuple

import pytest
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.utils import add_config_to_env
from dlt.common.runners.synth_pickle import decode_obj
from dlt.common.storages import FileStorage
from dlt.common.typing import TTimeInterval
from dlt.common.utils import uniq_id

from dlt.destinations.impl.postgres.configuration import PostgresCredentials
from dlt.helpers.dbt.dbt_utils import (
    DBTProcessingError,
    initialize_dbt_logging,
    run_dbt_command,
    is_incremental_schema_out_of_sync_error,
)
from dlt.helpers.dbt.runner import _event_time_args

from tests.utils import test_storage, preserve_environ
from tests.helpers.dbt_tests.utils import clone_jaffle_repo, load_test_case


def test_is_incremental_schema_out_of_sync_error() -> None:
    # in case of --fail-fast detect on a single run result
    assert (
        is_incremental_schema_out_of_sync_error(
            decode_obj(load_test_case("run_result_incremental_fail.pickle.hex"))
        )
        is True
    )
    assert (
        is_incremental_schema_out_of_sync_error(
            decode_obj(load_test_case("run_execution_incremental_fail.pickle.hex"))
        )
        is True
    )
    assert is_incremental_schema_out_of_sync_error("AAA") is False


@pytest.mark.parametrize(
    "interval,expected",
    [
        (
            (
                datetime(2024, 1, 3, tzinfo=timezone.utc),
                datetime(2024, 1, 6, tzinfo=timezone.utc),
            ),
            ("2024-01-03T00:00:00", "2024-01-06T00:00:00"),
        ),
        (
            TTimeInterval(
                datetime(2024, 1, 3, 1, 0, tzinfo=timezone(timedelta(hours=2))),
                datetime(2024, 1, 3, 3, 0, tzinfo=timezone(timedelta(hours=2))),
            ),
            ("2024-01-02T23:00:00", "2024-01-03T01:00:00"),
        ),
        (
            (datetime(2024, 1, 3, 10, 0), datetime(2024, 1, 3, 11, 0)),
            ("2024-01-03T10:00:00", "2024-01-03T11:00:00"),
        ),
        (
            (
                datetime(2024, 1, 3, 10, 0, 5, 500, tzinfo=timezone.utc),
                datetime(2024, 1, 3, 11, 0, 5, 500, tzinfo=timezone.utc),
            ),
            ("2024-01-03T10:00:05", "2024-01-03T11:00:06"),
        ),
    ],
    ids=["utc", "non-utc-tz", "naive", "microseconds-widened"],
)
def test_event_time_args(interval: Any, expected: Tuple[str, str]) -> None:
    assert _event_time_args(interval) == [
        "--event-time-start",
        expected[0],
        "--event-time-end",
        expected[1],
    ]


def test_dbt_commands(test_storage: FileStorage) -> None:
    schema_name = "s_" + uniq_id()
    # profiles in cases require this var to be set
    dbt_vars = {"dbt_schema": schema_name}

    # extract postgres creds from env, parse and emit
    credentials = resolve_configuration(PostgresCredentials(), sections=("destination", "postgres"))
    add_config_to_env(credentials, ("dlt",))

    repo_path = clone_jaffle_repo(test_storage)
    # copy profile
    shutil.copy(
        "./tests/helpers/dbt_tests/cases/profiles_invalid_credentials.yml",
        os.path.join(repo_path, "profiles.yml"),
    )
    # initialize logging
    global_args = initialize_dbt_logging("ERROR", False)
    # run deps, results are None
    assert run_dbt_command(repo_path, "deps", ".", global_args=global_args) is None

    # run list, results are list of strings
    results = run_dbt_command(
        repo_path, "list", ".", global_args=global_args, package_vars=dbt_vars
    )
    assert isinstance(results, list)
    assert len(results) == 28
    assert "jaffle_shop.not_null_orders_amount" in results
    # run list for specific selector
    results = run_dbt_command(
        repo_path,
        "list",
        ".",
        global_args=global_args,
        command_args=["-s", "jaffle_shop.not_null_orders_amount"],
        package_vars=dbt_vars,
    )
    assert len(results) == 1
    assert results[0] == "jaffle_shop.not_null_orders_amount"
    # run debug, that will fail
    with pytest.raises(DBTProcessingError) as dbt_err:
        run_dbt_command(repo_path, "debug", ".", global_args=global_args, package_vars=dbt_vars)
    # results are bool
    assert dbt_err.value.command == "debug"

    # we have no database connectivity so tests will fail
    with pytest.raises(DBTProcessingError) as dbt_err:
        run_dbt_command(repo_path, "test", ".", global_args=global_args, package_vars=dbt_vars)
    # in that case test results are bool, not list of tests runs
    assert dbt_err.value.command == "test"

    # same for run
    with pytest.raises(DBTProcessingError) as dbt_err:
        run_dbt_command(
            repo_path,
            "run",
            ".",
            global_args=global_args,
            package_vars=dbt_vars,
            command_args=["--fail-fast", "--full-refresh"],
        )
    # in that case test results are bool, not list of tests runs
    assert dbt_err.value.command == "run"

    # copy a correct profile
    shutil.copy(
        "./tests/helpers/dbt_tests/cases/profiles.yml", os.path.join(repo_path, "profiles.yml")
    )

    results = run_dbt_command(
        repo_path, "seed", ".", global_args=global_args, package_vars=dbt_vars
    )
    assert isinstance(results, list)
    assert len(results) == 3
    assert results[0].model_name == "raw_customers"
    assert results[0].status == "success"
    assert results[0].rows_affected == 100

    results = run_dbt_command(
        repo_path,
        "run",
        ".",
        global_args=global_args,
        package_vars=dbt_vars,
        command_args=["--fail-fast", "--full-refresh"],
    )
    assert isinstance(results, list)
    assert len(results) == 5
    assert results[-1].model_name == "orders"
    assert results[-1].status == "success"
    assert results[-1].rows_affected == 99

    results = run_dbt_command(
        repo_path, "test", ".", global_args=global_args, package_vars=dbt_vars
    )
    assert isinstance(results, list)
    assert len(results) == 20
    assert results[-1].status == "pass"
    assert results[-1].failures == 0
    # anonymous usage stats are disabled so dbt does not write its cookie into profiles dir
    assert not os.path.exists(os.path.join(repo_path, ".user.yml"))
