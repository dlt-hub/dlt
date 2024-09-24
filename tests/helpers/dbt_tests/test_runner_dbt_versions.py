import os
import shutil
import tempfile
from typing import Any, Iterator, List, Dict
from functools import partial
from typing import Tuple
import pytest
from dlt.common import json

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import GcpServiceAccountCredentials, CredentialsWithDefault
from dlt.common.storages.file_storage import FileStorage
from dlt.common.runners import Venv
from dlt.common.runners.synth_pickle import decode_obj, encode_obj
from dlt.common.typing import AnyFun

from dlt.destinations.impl.postgres.postgres import PostgresClient
from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration
from dlt.helpers.dbt.configuration import DBTRunnerConfiguration
from dlt.helpers.dbt.exceptions import PrerequisitesException, DBTProcessingError
from dlt.helpers.dbt import (
    package_runner,
    create_venv,
    _create_dbt_deps,
    _default_profile_name,
    DEFAULT_DBT_VERSION,
)

from tests.helpers.dbt_tests.utils import (
    JAFFLE_SHOP_REPO,
    assert_jaffle_completed,
    clone_jaffle_repo,
    find_run_result,
)

from tests.utils import test_storage, preserve_environ
from tests.load.utils import yield_client_with_storage, cm_yield_client_with_storage


@pytest.fixture(scope="function")
def client() -> Iterator[PostgresClient]:
    yield from yield_client_with_storage("postgres")  # type: ignore[misc]


PACKAGE_PARAMS = [
    ("postgres", "1.5.2"),
    ("postgres", "1.6.13"),
    ("postgres", "1.8.1"),
    ("postgres", None),
    ("snowflake", "1.5.2"),
    ("snowflake", "1.6.13"),
    ("snowflake", "1.8.1"),
    ("snowflake", None),
]
PACKAGE_IDS = [
    f"{destination}-venv-{version}" if version else f"{destination}-local"
    for destination, version in PACKAGE_PARAMS
]


@pytest.fixture(scope="module", params=PACKAGE_PARAMS, ids=PACKAGE_IDS)
def dbt_package_f(request: Any) -> Iterator[Tuple[str, AnyFun]]:
    destination_name, version = request.param
    if version is None:
        yield destination_name, partial(package_runner, Venv.restore_current())
    else:
        with create_venv(tempfile.mkdtemp(), [destination_name], version) as venv:
            yield destination_name, partial(package_runner, venv)


def test_infer_venv_deps() -> None:
    requirements = _create_dbt_deps(["postgres", "mssql"])
    assert requirements[:3] == [f"dbt-core{DEFAULT_DBT_VERSION}", "dbt-postgres", "dbt-sqlserver"]
    # should lead to here
    assert os.path.isdir(requirements[-1])
    # provide exact version
    requirements = _create_dbt_deps(["postgres"], dbt_version="3.3.3")
    assert requirements[:-1] == ["dbt-core==3.3.3", "dbt-postgres"]
    # provide version ranges
    requirements = _create_dbt_deps(["duckdb"], dbt_version=">3")
    # special duckdb dependency
    assert requirements[:-1] == ["dbt-core>3", "dbt-duckdb", "duckdb==1.1.0"]
    # we do not validate version ranges, pip will do it and fail when creating venv
    requirements = _create_dbt_deps(["motherduck"], dbt_version="y")
    assert requirements[:-1] == ["dbt-corey", "dbt-duckdb", "duckdb==1.1.0"]


def test_default_profile_name() -> None:
    bigquery_config = BigQueryClientConfiguration(credentials=GcpServiceAccountCredentials())
    assert isinstance(bigquery_config.credentials, CredentialsWithDefault)
    # default credentials are not present
    assert _default_profile_name(bigquery_config) == "bigquery"
    # force them to be present
    bigquery_config.credentials._set_default_credentials({})
    assert _default_profile_name(bigquery_config) == "bigquery_default"


def test_dbt_configuration() -> None:
    # check names normalized
    C: DBTRunnerConfiguration = resolve_configuration(
        DBTRunnerConfiguration(),
        explicit_value={
            "package_repository_ssh_key": "---NO NEWLINE---",
            "package_location": "/var/local",
        },
    )
    assert C.package_repository_ssh_key == "---NO NEWLINE---\n"
    assert C.package_additional_vars is None
    # profiles are set to the module dir
    assert C.package_profiles_dir.endswith("dbt")

    C = resolve_configuration(
        DBTRunnerConfiguration(),
        explicit_value={
            "package_repository_ssh_key": "---WITH NEWLINE---\n",
            "package_location": "/var/local",
            "package_additional_vars": {"a": 1},
        },
    )
    assert C.package_repository_ssh_key == "---WITH NEWLINE---\n"
    assert C.package_additional_vars == {"a": 1}


def test_dbt_run_exception_pickle() -> None:
    obj = decode_obj(
        encode_obj(
            DBTProcessingError("test", "A", "B"),  # type: ignore[arg-type]
            ignore_pickle_errors=False,
        ),
        ignore_pickle_errors=False,
    )
    assert obj.command == "test"
    assert obj.run_results == "A"
    assert obj.dbt_results == "B"
    assert str(obj) == "DBT command test could not be executed"


def test_runner_setup(client: PostgresClient, test_storage: FileStorage) -> None:
    add_vars = {
        "source_dataset_name": "overwritten",
        "destination_dataset_name": "destination",
        "schema_name": "this_Schema",
    }
    os.environ["DBT_PACKAGE_RUNNER__PACKAGE_ADDITIONAL_VARS"] = json.dumps(add_vars)
    os.environ["AUTO_FULL_REFRESH_WHEN_OUT_OF_SYNC"] = "False"
    os.environ["DBT_PACKAGE_RUNNER__RUNTIME__LOG_LEVEL"] = "CRITICAL"
    test_storage.create_folder("jaffle")
    r = package_runner(
        Venv.restore_current(),
        client.config,
        test_storage.make_full_path("jaffle"),
        JAFFLE_SHOP_REPO,
    )
    # runner settings
    assert r.credentials is client.config
    assert r.working_dir == test_storage.make_full_path("jaffle")
    assert r.source_dataset_name == client.config.dataset_name
    assert client.config.dataset_name.startswith("test")
    # runner config init
    assert r.config.package_location == JAFFLE_SHOP_REPO
    assert r.config.package_repository_branch is None
    assert r.config.package_repository_ssh_key == ""
    assert r.config.package_profile_name == "postgres"
    assert r.config.package_profiles_dir.endswith("dbt")
    assert r.config.package_additional_vars == add_vars
    assert r.config.runtime.log_level == "CRITICAL"
    assert r.config.auto_full_refresh_when_out_of_sync is False

    assert r._get_package_vars() == {
        "source_dataset_name": client.config.dataset_name,
        "destination_dataset_name": "destination",
        "schema_name": "this_Schema",
    }
    assert r._get_package_vars(destination_dataset_name="dest_test_123") == {
        "source_dataset_name": client.config.dataset_name,
        "destination_dataset_name": "dest_test_123",
        "schema_name": "this_Schema",
    }
    assert r._get_package_vars(additional_vars={"add": 1, "schema_name": "ovr"}) == {
        "source_dataset_name": client.config.dataset_name,
        "destination_dataset_name": "destination",
        "schema_name": "ovr",
        "add": 1,
    }


def test_runner_dbt_destinations(
    test_storage: FileStorage, dbt_package_f: Tuple[str, AnyFun]
) -> None:
    destination_name, dbt_func = dbt_package_f
    with cm_yield_client_with_storage(destination_name) as client:
        jaffle_base_dir = "jaffle_" + destination_name
        test_storage.create_folder(jaffle_base_dir)
        results = dbt_func(
            client.config, test_storage.make_full_path(jaffle_base_dir), JAFFLE_SHOP_REPO
        ).run_all(["--fail-fast", "--full-refresh"])
        assert_jaffle_completed(
            test_storage, results, destination_name, jaffle_dir=jaffle_base_dir + "/jaffle_shop"
        )


def test_run_jaffle_from_folder_incremental(
    test_storage: FileStorage, dbt_package_f: Tuple[str, AnyFun]
) -> None:
    destination_name, dbt_func = dbt_package_f
    with cm_yield_client_with_storage(destination_name) as client:
        repo_path = clone_jaffle_repo(test_storage)
        # copy model with error into package to force run error in model
        shutil.copy(
            "./tests/helpers/dbt_tests/cases/jaffle_customers_incremental.sql",
            os.path.join(repo_path, "models", "customers.sql"),
        )
        results = dbt_func(client.config, None, repo_path).run_all(run_params=None)
        assert_jaffle_completed(test_storage, results, destination_name, jaffle_dir="jaffle_shop")
        results = dbt_func(client.config, None, repo_path).run_all()
        # out of 100 records 0 was inserted
        customers = find_run_result(results, "customers")
        assert customers.message in JAFFLE_MESSAGES_INCREMENTAL[destination_name]["customers"]
        # change the column name. that will force dbt to fail (on_schema_change='fail'). the runner should do a full refresh
        shutil.copy(
            "./tests/helpers/dbt_tests/cases/jaffle_customers_incremental_new_column.sql",
            os.path.join(repo_path, "models", "customers.sql"),
        )
        results = dbt_func(client.config, None, repo_path).run_all(run_params=None)
        assert_jaffle_completed(test_storage, results, destination_name, jaffle_dir="jaffle_shop")


def test_run_jaffle_fail_prerequisites(
    test_storage: FileStorage, dbt_package_f: Tuple[str, AnyFun]
) -> None:
    destination_name, dbt_func = dbt_package_f
    with cm_yield_client_with_storage(destination_name) as client:
        test_storage.create_folder("jaffle")
        # we run all the tests before tables are materialized
        with pytest.raises(PrerequisitesException) as pr_exc:
            dbt_func(
                client.config, test_storage.make_full_path("jaffle"), JAFFLE_SHOP_REPO
            ).run_all(["--fail-fast", "--full-refresh"], source_tests_selector="*")
        proc_err = pr_exc.value.args[0]
        assert isinstance(proc_err, DBTProcessingError)
        customers = find_run_result(proc_err.run_results, "unique_customers_customer_id")
        assert customers.status == "error"
        assert len(proc_err.run_results) == 20
        assert all(r.status == "error" for r in proc_err.run_results)


def test_run_jaffle_invalid_run_args(
    test_storage: FileStorage, dbt_package_f: Tuple[str, AnyFun]
) -> None:
    destination_name, dbt_func = dbt_package_f
    with cm_yield_client_with_storage(destination_name) as client:
        test_storage.create_folder("jaffle")
        # we run all the tests before tables are materialized
        with pytest.raises(DBTProcessingError) as pr_exc:
            dbt_func(
                client.config, test_storage.make_full_path("jaffle"), JAFFLE_SHOP_REPO
            ).run_all(["--wrong_flag"])
        # dbt < 1.5 raises systemexit, dbt >= 1.5 just returns success False
        assert isinstance(pr_exc.value.dbt_results, SystemExit) or pr_exc.value.dbt_results is None


def test_run_jaffle_failed_run(
    test_storage: FileStorage, dbt_package_f: Tuple[str, AnyFun]
) -> None:
    destination_name, dbt_func = dbt_package_f
    with cm_yield_client_with_storage(destination_name) as client:
        repo_path = clone_jaffle_repo(test_storage)
        # copy model with error into package to force run error in model
        shutil.copy(
            "./tests/helpers/dbt_tests/cases/jaffle_customers_with_error.sql",
            os.path.join(repo_path, "models", "customers.sql"),
        )
        with pytest.raises(DBTProcessingError) as pr_exc:
            dbt_func(client.config, None, repo_path).run_all(run_params=None)
        assert len(pr_exc.value.run_results) == 5
        customers = find_run_result(pr_exc.value.run_results, "customers")
        assert customers.status == "error"


JAFFLE_MESSAGES_INCREMENTAL: Dict[str, Any] = {
    "snowflake": {
        # Different message per version
        "customers": ("SUCCESS 1", "SUCCESS 100"),
    },
    "postgres": {"customers": ("INSERT 0 100",)},
}
