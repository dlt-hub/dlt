import os
import shutil
import tempfile
from datetime import datetime, timezone
from typing import Any, Iterator, List, Dict
from functools import partial
from typing import Tuple
import pytest

import dlt
from dlt.common import json
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import GcpServiceAccountCredentials, CredentialsWithDefault
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.load_package import create_load_id
from dlt.common.runners import Venv
from dlt.common.runners.synth_pickle import decode_obj, encode_obj
from dlt.common.typing import AnyFun
from dlt.common.utils import uniq_id
from dlt.extract.incremental.context import TimeIntervalContext

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
    TEST_CASES_PATH,
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
    ("postgres", "1.9.3"),
    ("postgres", "1.10.19"),
    ("postgres", "1.11.6"),
    ("postgres", None),
    ("snowflake", "1.9.3"),
    ("snowflake", "1.10.19"),
    ("snowflake", "1.11.6"),
    ("snowflake", None),
]
PACKAGE_IDS = [
    f"{destination}-venv-{version}" if version else f"{destination}-local"
    for destination, version in PACKAGE_PARAMS
]

SNOWFLAKE_PKEY_DER_B64 = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQ=="
"""Stands in for a base64 DER key: the profile only passes it through, never parses it."""


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
    assert requirements[:-1] == ["dbt-core>3", "dbt-duckdb", "duckdb==1.5.3"]
    # we do not validate version ranges, pip will do it and fail when creating venv
    requirements = _create_dbt_deps(["motherduck"], dbt_version="y")
    assert requirements[:-1] == ["dbt-corey", "dbt-duckdb", "duckdb==1.5.3"]


def test_default_profile_name() -> None:
    bigquery_config = BigQueryClientConfiguration(credentials=GcpServiceAccountCredentials())
    assert isinstance(bigquery_config.credentials, CredentialsWithDefault)
    # default credentials are not present
    assert _default_profile_name(bigquery_config) == "bigquery"
    # force them to be present
    bigquery_config.credentials._set_default_credentials({})
    assert _default_profile_name(bigquery_config) == "bigquery_default"


def _render_profile(profile_name: str) -> Dict[str, Any]:
    """One profile of the shipped `profiles.yml`, as dbt renders it against the environment."""
    import yaml
    from jinja2 import Environment

    import dlt.helpers.dbt as dbt_helper

    def env_var(name: str, *default: Any) -> Any:
        # dbt refuses an `env_var` without a default when the variable is unset
        if default:
            return os.environ.get(name, default[0])
        return os.environ[name]

    path = os.path.join(os.path.dirname(dbt_helper.__file__), "profiles.yml")
    with open(path, "r", encoding="utf-8") as f:
        # the jinja lives inside quoted scalars, so the file parses as YAML before rendering
        outputs = yaml.safe_load(f)[profile_name]["outputs"]["analytics"]
    env = Environment()
    return {
        key: (
            env.from_string(value).render(
                env_var=env_var, var=lambda name, *default: default[0] if default else name
            )
            if isinstance(value, str)
            else value
        )
        for key, value in outputs.items()
    }


@pytest.mark.parametrize("with_password", [True, False], ids=["with-password", "key-only"])
def test_snowflake_pkey_profile_renders(preserve_environ: Any, with_password: bool) -> None:
    """The `snowflake_pkey` profile renders the private key, and a password only when one is set."""
    from dlt.common.configuration.utils import add_config_to_env
    from dlt.destinations.impl.snowflake.configuration import (
        SnowflakeClientConfiguration,
        SnowflakeCredentials,
    )

    credentials = SnowflakeCredentials()
    credentials.host = "acct"
    credentials.database = "db"
    credentials.username = "loader"
    credentials.private_key = SNOWFLAKE_PKEY_DER_B64
    if with_password:
        credentials.password = "pwd"
    credentials.resolve()
    config = SnowflakeClientConfiguration(credentials=credentials)._bind_dataset_name("ds")
    assert _default_profile_name(config) == "snowflake_pkey"

    os.environ["DLT__CREDENTIALS__PASSWORD"] = "stale"
    add_config_to_env(credentials, ("dlt",))
    assert ("DLT__CREDENTIALS__PASSWORD" in os.environ) is with_password

    output = _render_profile("snowflake_pkey")
    assert output["private_key"] == SNOWFLAKE_PKEY_DER_B64
    assert output["password"] == ("pwd" if with_password else "")


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
    assert str(obj) == "DBT command `test` could not be executed"


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
    assert r._get_package_vars(load_id="1712345678.123") == {
        "source_dataset_name": client.config.dataset_name,
        "destination_dataset_name": "destination",
        "schema_name": "this_Schema",
        "load_id": "1712345678.123",
    }
    # explicit vars override the load id
    assert (
        r._get_package_vars(additional_vars={"load_id": "ovr"}, load_id="1712345678.123")["load_id"]
        == "ovr"
    )


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
        # no load id was passed
        assert all(r.load_id is None for r in proc_err.run_results)


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
        # dbt >= 1.7 returns success False via dbtRunner, but SystemExit is handled defensively
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


def test_run_microbatch_with_interval(
    test_storage: FileStorage, dbt_package_f: Tuple[str, AnyFun]
) -> None:
    """Loads with an injected interval and transforms the same interval with a dbt microbatch model."""
    destination_name, dbt_func = dbt_package_f
    if destination_name != "postgres":
        pytest.skip("microbatch flow is tested on postgres only")

    @dlt.resource(primary_key="id", write_disposition="merge")
    def events(
        created_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
            "created_at", allow_external_schedulers=True
        )
    ) -> Iterator[Dict[str, Any]]:
        for day in range(1, 11):
            yield {"id": day, "created_at": datetime(2024, 1, day, 12, tzinfo=timezone.utc)}

    package_path = test_storage.make_full_path("microbatch_package")
    shutil.copytree(os.path.join(TEST_CASES_PATH, "microbatch_package"), package_path)
    pipeline = dlt.pipeline(
        "dbt_microbatch_" + uniq_id(),
        destination=destination_name,
        dataset_name="test_dbt_microbatch_" + uniq_id(),
    )

    def _select_model() -> List[Tuple[Any, ...]]:
        with pipeline.sql_client() as client:
            table_name = client.make_qualified_table_name("events_microbatch")
            rows = client.execute_sql(f"SELECT id, _dlt_load_id FROM {table_name} ORDER BY id")
            return [tuple(row) for row in rows]

    try:
        # load all 10 days, then merge days 3-5 again in a new load package
        with Container().injectable_context(
            TimeIntervalContext(interval=(datetime(2024, 1, 1), datetime(2024, 1, 11)))
        ):
            pipeline.run(events())
        with Container().injectable_context(
            TimeIntervalContext(interval=(datetime(2024, 1, 3), datetime(2024, 1, 6)))
        ):
            source_load_id = pipeline.run(events()).loads_ids[0]
            # models write the load id of the data they produce, not the one they process
            load_id = create_load_id()
            runner = dbt_func(pipeline.destination_client().config, None, package_path)
            models = runner.run_all(interval=dlt.current.interval(), load_id=load_id)
        assert load_id != source_load_id
        assert [m.load_id for m in models] == [load_id, load_id]
        # only batches within the interval were materialized
        assert _select_model() == [(3, load_id), (4, load_id), (5, load_id)]
        # postgres reports rows of a table model, dbt drops responses of microbatch batches
        assert find_run_result(models, "events_table").rows_affected == 10
        assert find_run_result(models, "events_microbatch").rows_affected is None
        tests = runner.test()
        assert find_run_result(tests, "events_before_jan_3").status == "pass"
        assert find_run_result(tests, "events_before_jan_3").failures == 0

        # without load id the package macro falls back to the dbt invocation start time
        models = runner.run_all(interval=(datetime(2024, 1, 1), datetime(2024, 1, 3)))
        assert all(m.load_id is None for m in models)
        # dbt anonymous usage stats are disabled by default, no cookie in profiles dir
        assert not runner.config.send_anonymous_usage_stats
        assert not os.path.exists(os.path.join(runner.config.package_profiles_dir, ".user.yml"))
        generated_load_id = _select_model()[0][1]
        assert float(generated_load_id) > float(load_id)
        assert _select_model() == [
            (1, generated_load_id),
            (2, generated_load_id),
            (3, load_id),
            (4, load_id),
            (5, load_id),
        ]
        # warn test reports the rows from before 01-03 as failures
        tests = runner.test()
        assert find_run_result(tests, "events_before_jan_3").status == "warn"
        assert find_run_result(tests, "events_before_jan_3").failures == 2
    finally:
        with pipeline.sql_client() as client:
            client.drop_dataset()


JAFFLE_MESSAGES_INCREMENTAL: Dict[str, Any] = {
    "snowflake": {
        # dbt < 1.10 returns "SUCCESS 1", dbt >= 1.10 returns actual row counts
        "customers": ("SUCCESS 1", "SUCCESS 0", "SUCCESS 100"),
    },
    "postgres": {"customers": ("INSERT 0 100",)},
}
