import pytest
import tempfile
import shutil
from importlib.metadata import version as pkg_version

import dlt
from dlt.common import json
from dlt.common.runners import Venv
from dlt.common.storages.exceptions import StorageMigrationError
from dlt.common.utils import custom_environ, set_working_dir
from dlt.common.configuration.paths import get_dlt_data_dir
from dlt.common.storages import FileStorage
from dlt.common.schema.typing import (
    LOADS_TABLE_NAME,
    STATE_TABLE_NAME,
    VERSION_TABLE_NAME,
    TStoredSchema,
)
from dlt.common.configuration.resolve import resolve_configuration
from dlt.destinations.impl.duckdb.configuration import DuckDbClientConfiguration
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient

from tests.utils import TEST_STORAGE_ROOT, test_storage


GITHUB_PIPELINE_NAME = "dlt_github_pipeline"
GITHUB_DATASET = "github_3"


def test_pipeline_with_dlt_update(test_storage: FileStorage) -> None:
    shutil.copytree("tests/pipeline/cases/github_pipeline", TEST_STORAGE_ROOT, dirs_exist_ok=True)

    # execute in test storage
    with set_working_dir(TEST_STORAGE_ROOT):
        # store dlt data in test storage (like patch_home_dir)
        with custom_environ({"DLT_DATA_DIR": get_dlt_data_dir()}):
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
                            "../tests/pipeline/cases/github_pipeline/github_pipeline.py", "20"
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
                        "version_hash" not in github_schema["tables"][STATE_TABLE_NAME]["columns"]
                    }
                    # check loads table without attaching to pipeline
                    duckdb_cfg = resolve_configuration(
                        DuckDbClientConfiguration(dataset_name=GITHUB_DATASET),
                        sections=("destination", "duckdb"),
                    )
                    with DuckDbSqlClient(GITHUB_DATASET, duckdb_cfg.credentials) as client:
                        rows = client.execute_sql(f"SELECT * FROM {LOADS_TABLE_NAME}")
                        # make sure we have just 4 columns
                        assert len(rows[0]) == 4
                        rows = client.execute_sql("SELECT * FROM issues")
                        assert len(rows) == 20
                        rows = client.execute_sql(f"SELECT * FROM {STATE_TABLE_NAME}")
                        # only 5 columns + 2 dlt columns
                        assert len(rows[0]) == 5 + 2
                    # inspect old state
                    state_dict = json.loads(
                        test_storage.load(f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/state.json")
                    )
                    assert "_version_hash" not in state_dict
                    assert (
                        state_dict["sources"]["github"]["resources"]["load_issues"]["incremental"][
                            "created_at"
                        ]["last_value"]
                        == "2021-04-16T04:34:05Z"
                    )

                # execute in current version
                venv = Venv.restore_current()
                # load all issues
                print(venv.run_script("../tests/pipeline/cases/github_pipeline/github_pipeline.py"))
                # hash hash in schema
                github_schema = json.loads(
                    test_storage.load(
                        f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/schemas/github.schema.json"
                    )
                )
                assert github_schema["engine_version"] == 8
                assert "schema_version_hash" in github_schema["tables"][LOADS_TABLE_NAME]["columns"]
                # load state
                state_dict = json.loads(
                    test_storage.load(f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/state.json")
                )
                assert "_version_hash" in state_dict

                with DuckDbSqlClient(GITHUB_DATASET, duckdb_cfg.credentials) as client:
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
                    rows = client.execute_sql(f"SELECT * FROM {STATE_TABLE_NAME} ORDER BY version")
                    # we have hash columns
                    assert len(rows[0]) == 6 + 2
                    assert len(rows) == 2
                    # none inserted after schema migration in load 1
                    assert rows[0][7] is None
                    # hash of schema present in current load (2)
                    assert rows[1][7] == state_dict["_version_hash"]

                # attach to existing pipeline
                pipeline = dlt.attach(GITHUB_PIPELINE_NAME, credentials=duckdb_cfg.credentials)
                assert (
                    pipeline.state["sources"]["github"]["resources"]["load_issues"]["incremental"][
                        "created_at"
                    ]["last_value"]
                    == "2023-02-17T09:52:12Z"
                )
                pipeline = pipeline.drop()
                # print(pipeline.working_dir)
                assert pipeline.dataset_name == GITHUB_DATASET
                assert pipeline.default_schema_name is None
                # sync from destination
                pipeline.sync_destination()
                # print(pipeline.working_dir)
                # we have updated schema
                assert pipeline.default_schema.ENGINE_VERSION == 8
                # make sure that schema hash retrieved from the destination is exactly the same as the schema hash that was in storage before the schema was wiped
                assert pipeline.default_schema.stored_version_hash == github_schema["version_hash"]


def test_load_package_with_dlt_update(test_storage: FileStorage) -> None:
    shutil.copytree("tests/pipeline/cases/github_pipeline", TEST_STORAGE_ROOT, dirs_exist_ok=True)

    # execute in test storage
    with set_working_dir(TEST_STORAGE_ROOT):
        # store dlt data in test storage (like patch_home_dir)
        with custom_environ({"DLT_DATA_DIR": get_dlt_data_dir()}):
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
                            "../tests/pipeline/cases/github_pipeline/github_normalize.py",
                        )
                    )
                # switch to current version and make sure the load package loads and schema migrates
                venv = Venv.restore_current()
                print(venv.run_script("../tests/pipeline/cases/github_pipeline/github_load.py"))
                duckdb_cfg = resolve_configuration(
                    DuckDbClientConfiguration(dataset_name=GITHUB_DATASET),
                    sections=("destination", "duckdb"),
                )
                with DuckDbSqlClient(GITHUB_DATASET, duckdb_cfg.credentials) as client:
                    rows = client.execute_sql("SELECT * FROM issues")
                    assert len(rows) == 70
                github_schema = json.loads(
                    test_storage.load(
                        f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/schemas/github.schema.json"
                    )
                )
                # attach to existing pipeline
                pipeline = dlt.attach(GITHUB_PIPELINE_NAME, credentials=duckdb_cfg.credentials)
                # get the schema from schema storage before we sync
                github_schema = json.loads(
                    test_storage.load(
                        f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/schemas/github.schema.json"
                    )
                )
                pipeline = pipeline.drop()
                pipeline.sync_destination()
                assert pipeline.default_schema.ENGINE_VERSION == 8
                # schema version does not match `dlt.attach` does not update to the right schema by itself
                assert pipeline.default_schema.stored_version_hash != github_schema["version_hash"]
                # state has hash
                assert pipeline.state["_version_hash"] is not None
                # but in db there's no hash - we loaded an old package with backward compatible schema
                with pipeline.sql_client() as client:
                    rows = client.execute_sql(f"SELECT * FROM {STATE_TABLE_NAME}")
                    # no hash
                    assert len(rows[0]) == 5 + 2
                    assert len(rows) == 1
                    # this will extract state and update state schema
                    with pipeline.managed_state(extract_state=True):
                        pass
                    # this will sync schema to destination
                    pipeline.sync_schema()
                    # we have hash now
                    rows = client.execute_sql(f"SELECT * FROM {STATE_TABLE_NAME}")
                    assert len(rows[0]) == 6 + 2


def test_normalize_package_with_dlt_update(test_storage: FileStorage) -> None:
    shutil.copytree("tests/pipeline/cases/github_pipeline", TEST_STORAGE_ROOT, dirs_exist_ok=True)

    # execute in test storage
    with set_working_dir(TEST_STORAGE_ROOT):
        # store dlt data in test storage (like patch_home_dir)
        with custom_environ({"DLT_DATA_DIR": get_dlt_data_dir()}):
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
