import sys
from subprocess import CalledProcessError
import pytest
import tempfile
import shutil
from unittest.mock import patch
from importlib.metadata import version as pkg_version

import dlt
from dlt.common import json, pendulum
from dlt.common.known_env import DLT_DATA_DIR
from dlt.common.json import custom_pua_decode
from dlt.common.runners import Venv
from dlt.common.storages.exceptions import StorageMigrationError
from dlt.common.utils import custom_environ, set_working_dir
from dlt.common.storages import FileStorage
from dlt.common.schema.typing import (
    LOADS_TABLE_NAME,
    PIPELINE_STATE_TABLE_NAME,
    SCHEMA_ENGINE_VERSION,
    VERSION_TABLE_NAME,
    TStoredSchema,
)
from dlt.common.configuration.resolve import resolve_configuration
from dlt.destinations import duckdb, filesystem
from dlt.destinations.impl.duckdb.configuration import DuckDbClientConfiguration
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient

from tests.pipeline.utils import airtable_emojis, load_table_counts
from tests.utils import TEST_STORAGE_ROOT


def test_simulate_default_naming_convention_change() -> None:
    # checks that (future) change in the naming convention won't affect existing pipelines
    pipeline = dlt.pipeline("simulated_snake_case", destination="duckdb")
    assert pipeline.naming.name() == "snake_case"
    pipeline.run(airtable_emojis().with_resources("📆 Schedule", "🦚Peacock", "🦚WidePeacock"))
    # normalized names
    assert pipeline.last_trace.last_normalize_info.row_counts["_schedule"] == 3
    assert "_schedule" in pipeline.default_schema.tables

    # mock the mod
    # from dlt.common.normalizers import utils

    with patch("dlt.common.schema.normalizers.DEFAULT_NAMING_MODULE", "duck_case"):
        duck_pipeline = dlt.pipeline("simulated_duck_case", destination="duckdb")
        assert duck_pipeline.naming.name() == "duck_case"
        print(airtable_emojis().schema.naming.name())

        # run new and old pipelines
        duck_pipeline.run(
            airtable_emojis().with_resources("📆 Schedule", "🦚Peacock", "🦚WidePeacock")
        )
        print(duck_pipeline.last_trace.last_normalize_info.row_counts)
        assert duck_pipeline.last_trace.last_normalize_info.row_counts["📆 Schedule"] == 3
        assert "📆 Schedule" in duck_pipeline.default_schema.tables

        # old pipeline should keep its naming convention
        pipeline.run(airtable_emojis().with_resources("📆 Schedule", "🦚Peacock", "🦚WidePeacock"))
        # normalized names
        assert pipeline.last_trace.last_normalize_info.row_counts["_schedule"] == 3
        assert pipeline.naming.name() == "snake_case"


if sys.version_info >= (3, 12):
    pytest.skip("Does not run on Python 3.12 and later", allow_module_level=True)


GITHUB_PIPELINE_NAME = "dlt_github_pipeline"
GITHUB_DATASET = "github_3"


def test_pipeline_with_dlt_update(test_storage: FileStorage) -> None:
    shutil.copytree("tests/pipeline/cases/github_pipeline", TEST_STORAGE_ROOT, dirs_exist_ok=True)

    # execute in test storage
    with set_working_dir(TEST_STORAGE_ROOT):
        # store dlt data in test storage (like patch_home_dir)
        with custom_environ({DLT_DATA_DIR: dlt.current.run().data_dir}):
            # save database outside of pipeline dir
            with custom_environ(
                {"DESTINATION__DUCKDB__CREDENTIALS": "duckdb:///test_github_3.duckdb"}
            ):
                # create virtual env with (0.3.0) before the current schema upgrade
                with Venv.create(tempfile.mkdtemp(), ["dlt[duckdb]==0.3.0"]) as venv:
                    # NOTE: we force a newer duckdb into the 0.3.0 dlt version to get compatible duckdb storage
                    venv._install_deps(venv.context, ["duckdb" + "==" + pkg_version("duckdb")])
                    # load 20 issues
                    print(
                        venv.run_script(
                            "../tests/pipeline/cases/github_pipeline/github_pipeline.py",
                            "duckdb",
                            "20",
                        )
                    )
                    # load schema and check _dlt_loads definition
                    github_schema: TStoredSchema = json.loads(
                        test_storage.load(
                            f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/schemas/github.schema.json"
                        )
                    )
                    assert github_schema["engine_version"] == 5
                    assert (
                        "schema_version_hash"
                        not in github_schema["tables"][LOADS_TABLE_NAME]["columns"]
                    )
                    # check the dlt state table
                    assert {
                        "version_hash"
                        not in github_schema["tables"][PIPELINE_STATE_TABLE_NAME]["columns"]
                    }
                    # make sure that assignees are complex
                    assert (
                        github_schema["tables"]["issues"]["columns"]["assignee"]["data_type"]
                        == "complex"
                    )
                    # check loads table without attaching to pipeline
                    duckdb_cfg = resolve_configuration(
                        DuckDbClientConfiguration()._bind_dataset_name(dataset_name=GITHUB_DATASET),
                        sections=("destination", "duckdb"),
                    )
                    with DuckDbSqlClient(
                        GITHUB_DATASET,
                        "%s_staging",
                        duckdb_cfg.credentials,
                        duckdb().capabilities(),
                    ) as client:
                        rows = client.execute_sql(f"SELECT * FROM {LOADS_TABLE_NAME}")
                        # make sure we have just 4 columns
                        assert len(rows[0]) == 4
                        rows = client.execute_sql("SELECT * FROM issues")
                        assert len(rows) == 20
                        rows = client.execute_sql(f"SELECT * FROM {PIPELINE_STATE_TABLE_NAME}")
                        # only 5 columns + 2 dlt columns
                        assert len(rows[0]) == 5 + 2
                    # inspect old state
                    state_dict = json.loads(
                        test_storage.load(f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/state.json")
                    )
                    assert "_version_hash" not in state_dict
                    # also we expect correctly decoded pendulum here
                    created_at_value = custom_pua_decode(
                        state_dict["sources"]["github"]["resources"]["load_issues"]["incremental"][
                            "created_at"
                        ]["last_value"]
                    )
                    assert isinstance(created_at_value, pendulum.DateTime)
                    assert created_at_value == pendulum.parse("2021-04-16T04:34:05Z")

                # execute in current version
                venv = Venv.restore_current()
                # load all issues
                try:
                    print(
                        venv.run_script(
                            "../tests/pipeline/cases/github_pipeline/github_pipeline.py", "duckdb"
                        )
                    )
                except CalledProcessError as cpe:
                    print(f"script stdout: {cpe.stdout}")
                    print(f"script stderr: {cpe.stderr}")
                    raise
                # hash hash in schema
                github_schema = json.loads(
                    test_storage.load(
                        f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/schemas/github.schema.json"
                    )
                )
                assert github_schema["engine_version"] == SCHEMA_ENGINE_VERSION
                assert (
                    github_schema["tables"]["issues"]["columns"]["assignee"]["data_type"] == "json"
                )
                assert "schema_version_hash" in github_schema["tables"][LOADS_TABLE_NAME]["columns"]
                # print(github_schema["tables"][PIPELINE_STATE_TABLE_NAME])
                # load state
                state_dict = json.loads(
                    test_storage.load(f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/state.json")
                )
                assert "_version_hash" in state_dict

                with DuckDbSqlClient(
                    GITHUB_DATASET, "%s_staging", duckdb_cfg.credentials, duckdb().capabilities()
                ) as client:
                    rows = client.execute_sql(
                        f"SELECT * FROM {LOADS_TABLE_NAME} ORDER BY inserted_at"
                    )
                    # we have two loads
                    assert len(rows) == 2
                    assert len(rows[0]) == 5
                    # none inserted after schema migration in load 1
                    assert rows[0][4] is None
                    # hash of schema present in current load (2)
                    assert rows[1][4] == github_schema["version_hash"]
                    # make sure all 100 records loaded (we do incremental load)
                    rows = client.execute_sql("SELECT * FROM issues")
                    assert len(rows) == 100
                    # two schema versions
                    rows = client.execute_sql(f"SELECT * FROM {VERSION_TABLE_NAME}")
                    assert len(rows) == 2
                    rows = client.execute_sql(
                        f"SELECT * FROM {PIPELINE_STATE_TABLE_NAME} ORDER BY version"
                    )
                    # we have hash columns
                    assert len(rows[0]) == 6 + 2
                    assert len(rows) == 2
                    # none inserted after schema migration in load 1
                    assert rows[0][7] is None
                    # hash of schema present in current load (2)
                    assert rows[1][7] == state_dict["_version_hash"]

                # attach to existing pipeline
                pipeline = dlt.attach(
                    GITHUB_PIPELINE_NAME, destination=duckdb(credentials=duckdb_cfg.credentials)
                )
                assert_github_pipeline_end_state(pipeline, github_schema, 2)


def test_filesystem_pipeline_with_dlt_update(test_storage: FileStorage) -> None:
    shutil.copytree("tests/pipeline/cases/github_pipeline", TEST_STORAGE_ROOT, dirs_exist_ok=True)

    # execute in test storage
    with set_working_dir(TEST_STORAGE_ROOT):
        # store dlt data in test storage (like patch_home_dir)
        with custom_environ({DLT_DATA_DIR: dlt.current.run().data_dir}):
            # create virtual env with (0.4.9) where filesystem started to store state
            with Venv.create(tempfile.mkdtemp(), ["dlt==0.4.9"]) as venv:
                try:
                    print(venv.run_script("github_pipeline.py", "filesystem", "20"))
                except CalledProcessError as cpe:
                    print(f"script stdout: {cpe.stdout}")
                    print(f"script stderr: {cpe.stderr}")
                    raise
            # load all issues
            venv = Venv.restore_current()
            try:
                print(venv.run_script("github_pipeline.py", "filesystem"))
            except CalledProcessError as cpe:
                print(f"script stdout: {cpe.stdout}")
                print(f"script stderr: {cpe.stderr}")
                raise
            # hash hash in schema
            github_schema = json.loads(
                test_storage.load(
                    f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/schemas/github.schema.json"
                )
            )
            # attach to existing pipeline
            pipeline = dlt.attach(GITHUB_PIPELINE_NAME, destination=filesystem("_storage/data"))
            # assert end state
            assert_github_pipeline_end_state(pipeline, github_schema, 2)
            # load new state
            fs_client = pipeline._fs_client()
            state_files = sorted(fs_client.list_table_files("_dlt_pipeline_state"))
            # first file is in old format
            state_1 = json.loads(fs_client.read_text(state_files[0], encoding="utf-8"))
            assert "dlt_load_id" in state_1
            # seconds is new
            state_2 = json.loads(fs_client.read_text(state_files[1], encoding="utf-8"))
            assert "_dlt_load_id" in state_2


def assert_github_pipeline_end_state(
    pipeline: dlt.Pipeline, orig_schema: TStoredSchema, schema_updates: int
) -> None:
    # get tables counts
    table_counts = load_table_counts(pipeline, *pipeline.default_schema.data_table_names())
    assert table_counts == {"issues": 100, "issues__assignees": 31, "issues__labels": 34}
    dlt_counts = load_table_counts(pipeline, *pipeline.default_schema.dlt_table_names())
    assert dlt_counts == {"_dlt_version": schema_updates, "_dlt_loads": 2, "_dlt_pipeline_state": 2}

    # check state
    created_at_value = pipeline.state["sources"]["github"]["resources"]["load_issues"][
        "incremental"
    ]["created_at"]["last_value"]
    assert isinstance(created_at_value, pendulum.DateTime)
    assert created_at_value == pendulum.parse("2023-02-17T09:52:12Z")
    pipeline = pipeline.drop()
    # print(pipeline.working_dir)
    assert pipeline.dataset_name == GITHUB_DATASET
    assert pipeline.default_schema_name is None
    # sync from destination
    pipeline.sync_destination()
    # print(pipeline.working_dir)
    # we have updated schema
    assert pipeline.default_schema.ENGINE_VERSION == SCHEMA_ENGINE_VERSION
    # make sure that schema hash retrieved from the destination is exactly the same as the schema hash that was in storage before the schema was wiped
    assert pipeline.default_schema.stored_version_hash == orig_schema["version_hash"]


def test_load_package_with_dlt_update(test_storage: FileStorage) -> None:
    shutil.copytree("tests/pipeline/cases/github_pipeline", TEST_STORAGE_ROOT, dirs_exist_ok=True)

    # execute in test storage
    with set_working_dir(TEST_STORAGE_ROOT):
        # store dlt data in test storage (like patch_home_dir)
        with custom_environ({DLT_DATA_DIR: dlt.current.run().data_dir}):
            # save database outside of pipeline dir
            with custom_environ(
                {"DESTINATION__DUCKDB__CREDENTIALS": "duckdb:///test_github_3.duckdb"}
            ):
                # create virtual env with (0.3.0) before the current schema upgrade
                with Venv.create(tempfile.mkdtemp(), ["dlt[duckdb]==0.3.0"]) as venv:
                    venv._install_deps(venv.context, ["duckdb" + "==" + pkg_version("duckdb")])
                    # extract and normalize on old version but DO NOT LOAD
                    print(
                        venv.run_script(
                            "../tests/pipeline/cases/github_pipeline/github_extract.py", "70"
                        )
                    )
                    print(
                        venv.run_script(
                            "../tests/pipeline/cases/github_pipeline/github_normalize.py"
                        )
                    )
                # switch to current version and make sure the load package loads and schema migrates
                venv = Venv.restore_current()
                print(venv.run_script("../tests/pipeline/cases/github_pipeline/github_load.py"))
                duckdb_cfg = resolve_configuration(
                    DuckDbClientConfiguration()._bind_dataset_name(dataset_name=GITHUB_DATASET),
                    sections=("destination", "duckdb"),
                )
                with DuckDbSqlClient(
                    GITHUB_DATASET, "%s_staging", duckdb_cfg.credentials, duckdb().capabilities()
                ) as client:
                    rows = client.execute_sql("SELECT * FROM issues")
                    assert len(rows) == 70
                github_schema = json.loads(
                    test_storage.load(
                        f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/schemas/github.schema.json"
                    )
                )
                # attach to existing pipeline
                pipeline = dlt.attach(
                    GITHUB_PIPELINE_NAME, destination=duckdb(credentials=duckdb_cfg.credentials)
                )
                # get the schema from schema storage before we sync
                github_schema = json.loads(
                    test_storage.load(
                        f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/schemas/github.schema.json"
                    )
                )
                pipeline = pipeline.drop()
                pipeline.sync_destination()
                assert pipeline.default_schema.ENGINE_VERSION == SCHEMA_ENGINE_VERSION
                # schema version does not match `dlt.attach` does not update to the right schema by itself
                assert pipeline.default_schema.stored_version_hash != github_schema["version_hash"]
                # state has hash
                assert pipeline.state["_version_hash"] is not None
                # but in db there's no hash - we loaded an old package with backward compatible schema
                with pipeline.sql_client() as client:
                    rows = client.execute_sql(f"SELECT * FROM {PIPELINE_STATE_TABLE_NAME}")
                    # no hash
                    assert len(rows[0]) == 5 + 2
                    assert len(rows) == 1
                    # this will extract state and update state schema
                    with pipeline.managed_state(extract_state=True):
                        pass
                    # this will sync schema to destination
                    pipeline.sync_schema()
                    # we have hash now
                    rows = client.execute_sql(f"SELECT * FROM {PIPELINE_STATE_TABLE_NAME}")
                    assert len(rows[0]) == 6 + 2


def test_normalize_package_with_dlt_update(test_storage: FileStorage) -> None:
    shutil.copytree("tests/pipeline/cases/github_pipeline", TEST_STORAGE_ROOT, dirs_exist_ok=True)

    # execute in test storage
    with set_working_dir(TEST_STORAGE_ROOT):
        # store dlt data in test storage (like patch_home_dir)
        with custom_environ({DLT_DATA_DIR: dlt.current.run().data_dir}):
            # save database outside of pipeline dir
            with custom_environ(
                {"DESTINATION__DUCKDB__CREDENTIALS": "duckdb:///test_github_3.duckdb"}
            ):
                # create virtual env with (0.3.0) before the current schema upgrade
                with Venv.create(tempfile.mkdtemp(), ["dlt[duckdb]==0.3.0"]) as venv:
                    venv._install_deps(venv.context, ["duckdb" + "==" + pkg_version("duckdb")])
                    # extract only
                    print(
                        venv.run_script(
                            "../tests/pipeline/cases/github_pipeline/github_extract.py", "70"
                        )
                    )
                # switch to current version and normalize existing extract package
                # here we test possible switch in package format between storages
                pipeline = dlt.attach(GITHUB_PIPELINE_NAME)
                with pytest.raises(StorageMigrationError) as mig_ex:
                    pipeline.normalize()
                assert mig_ex.value.from_version == "1.0.0"

                # delete all files in extracted folder
                for file in pipeline._pipeline_storage.list_folder_files("normalize/extracted"):
                    pipeline._pipeline_storage.delete(file)
                # now we can migrate the storage
                pipeline.normalize()
                assert pipeline._get_normalize_storage().version == "1.0.1"


def test_scd2_pipeline_update(test_storage: FileStorage) -> None:
    shutil.copytree("tests/pipeline/cases/github_pipeline", TEST_STORAGE_ROOT, dirs_exist_ok=True)

    # execute in test storage
    with set_working_dir(TEST_STORAGE_ROOT):
        # store dlt data in test storage (like patch_home_dir)
        with custom_environ({DLT_DATA_DIR: dlt.current.run().data_dir}):
            # save database outside of pipeline dir
            with custom_environ(
                {"DESTINATION__DUCKDB__CREDENTIALS": "duckdb:///test_github_3.duckdb"}
            ):
                # run scd2 pipeline on 0.4.10
                venv_dir = tempfile.mkdtemp()
                # venv_dir == "tmp/dlt0410"
                with Venv.create(venv_dir, ["dlt[duckdb]==0.4.10"]) as venv:
                    venv._install_deps(venv.context, ["duckdb" + "==" + pkg_version("duckdb")])

                    print(venv.run_script("../tests/pipeline/cases/github_pipeline/github_scd2.py"))
                    # get data from original db
                    duckdb_cfg = resolve_configuration(
                        DuckDbClientConfiguration()._bind_dataset_name(dataset_name=GITHUB_DATASET),
                        sections=("destination", "duckdb"),
                    )
                    with DuckDbSqlClient(
                        GITHUB_DATASET,
                        "%s_staging",
                        duckdb_cfg.credentials,
                        duckdb().capabilities(),
                    ) as client:
                        issues = client.execute_sql("SELECT * FROM issues ORDER BY id")
                        issues__assignees = client.execute_sql(
                            "SELECT * FROM issues__assignees ORDER BY id"
                        )
                        issues__labels = client.execute_sql(
                            "SELECT * FROM issues__labels ORDER BY id"
                        )

                        assert len(issues) == 100

                venv = Venv.restore_current()
                # load same data again
                print(venv.run_script("../tests/pipeline/cases/github_pipeline/github_scd2.py"))
                pipeline = dlt.attach(GITHUB_PIPELINE_NAME)
                # unique on row_key got swapped from True to False
                assert (
                    pipeline.default_schema.tables["issues"]["columns"]["_dlt_id"]["unique"]
                    is False
                )
                # datasets must be the same
                with DuckDbSqlClient(
                    GITHUB_DATASET,
                    "%s_staging",
                    duckdb_cfg.credentials,
                    duckdb().capabilities(),
                ) as client:
                    issues_n = client.execute_sql("SELECT * FROM issues ORDER BY id")
                    issues__assignees_n = client.execute_sql(
                        "SELECT * FROM issues__assignees ORDER BY id"
                    )
                    issues__labels_n = client.execute_sql(
                        "SELECT * FROM issues__labels ORDER BY id"
                    )
                assert issues == issues_n
                assert issues__assignees == issues__assignees_n
                assert issues__labels == issues__labels_n

                # retire some ids
                print(
                    venv.run_script(
                        "../tests/pipeline/cases/github_pipeline/github_scd2.py", "6272"
                    )
                )
                with DuckDbSqlClient(
                    GITHUB_DATASET,
                    "%s_staging",
                    duckdb_cfg.credentials,
                    duckdb().capabilities(),
                ) as client:
                    issues_retired = client.execute_sql(
                        "SELECT number FROM issues WHERE _dlt_valid_to IS NOT NULL"
                    )

                assert len(issues_retired) == 1
                assert issues_retired[0][0] == 6272
                # print(pipeline.default_schema.to_pretty_yaml())
