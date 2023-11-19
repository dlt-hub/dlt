import tempfile
import shutil
from importlib.metadata import version as pkg_version

import dlt
from dlt.common import json
from dlt.common.runners import Venv
from dlt.common.utils import custom_environ, set_working_dir
from dlt.common.configuration.paths import get_dlt_data_dir
from dlt.common.storages import FileStorage
from dlt.common.schema.typing import LOADS_TABLE_NAME, VERSION_TABLE_NAME, TStoredSchema
from dlt.common.configuration.resolve import resolve_configuration
from dlt.destinations.duckdb.configuration import DuckDbClientConfiguration
from dlt.destinations.duckdb.sql_client import DuckDbSqlClient

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
            with custom_environ({"DESTINATION__DUCKDB__CREDENTIALS": "duckdb:///test_github_3.duckdb"}):
                # create virtual env with (0.3.0) before the current schema upgrade
                with Venv.create(tempfile.mkdtemp(), ["dlt[duckdb]==0.3.0"]) as venv:
                    # NOTE: we force a newer duckdb into the 0.3.0 dlt version to get compatible duckdb storage
                    venv._install_deps(venv.context, ["duckdb" + "==" + pkg_version("duckdb")])
                    # load 20 issues
                    print(venv.run_script("../tests/pipeline/cases/github_pipeline/github_pipeline.py", "20"))
                    # load schema and check _dlt_loads definition
                    github_schema: TStoredSchema = json.loads(test_storage.load(f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/schemas/github.schema.json"))
                    # print(github_schema["tables"][LOADS_TABLE_NAME])
                    assert github_schema["engine_version"] == 5
                    assert "schema_version_hash" not in github_schema["tables"][LOADS_TABLE_NAME]["columns"]
                    # check loads table without attaching to pipeline
                    duckdb_cfg = resolve_configuration(DuckDbClientConfiguration(dataset_name=GITHUB_DATASET), sections=("destination", "duckdb"))
                    with DuckDbSqlClient(GITHUB_DATASET, duckdb_cfg.credentials) as client:
                        rows = client.execute_sql(f"SELECT * FROM {LOADS_TABLE_NAME}")
                        # make sure we have just 4 columns
                        assert len(rows[0]) == 4
                        rows = client.execute_sql("SELECT * FROM issues")
                        assert len(rows) == 20

                # execute in current version
                venv = Venv.restore_current()
                # load all issues
                print(venv.run_script("../tests/pipeline/cases/github_pipeline/github_pipeline.py"))
                # hash hash in schema
                github_schema = json.loads(test_storage.load(f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/schemas/github.schema.json"))
                assert github_schema["engine_version"] == 6
                assert "schema_version_hash" in github_schema["tables"][LOADS_TABLE_NAME]["columns"]
                with DuckDbSqlClient(GITHUB_DATASET, duckdb_cfg.credentials) as client:
                    rows = client.execute_sql(f"SELECT * FROM {LOADS_TABLE_NAME} ORDER BY inserted_at")
                    # we have two loads
                    assert len(rows) == 2
                    assert len(rows[0]) == 5
                    # none inserted after schema migration in load 1
                    assert rows[0][4] is None
                    # hash of schema present in current load (2)
                    assert rows[1][4] == github_schema["version_hash"]
                    rows = client.execute_sql("SELECT * FROM issues")
                    assert len(rows) == 100
                    # two schema versions
                    rows = client.execute_sql(f"SELECT * FROM {VERSION_TABLE_NAME}")
                    assert len(rows) == 2
                # attach to existing pipeline
                pipeline = dlt.attach(GITHUB_PIPELINE_NAME, credentials=duckdb_cfg.credentials)
                pipeline = pipeline.drop()
                # print(pipeline.working_dir)
                assert pipeline.dataset_name == GITHUB_DATASET
                assert pipeline.default_schema_name is None
                # sync from destination
                pipeline.sync_destination()
                # print(pipeline.working_dir)
                # we have updated schema
                assert pipeline.default_schema.ENGINE_VERSION == 7
                # make sure that schema hash retrieved from the destination is exactly the same as the schema hash that was in storage before the schema was wiped
                assert pipeline.default_schema.stored_version_hash == github_schema["version_hash"]


def test_load_package_with_dlt_update(test_storage: FileStorage) -> None:
    shutil.copytree("tests/pipeline/cases/github_pipeline", TEST_STORAGE_ROOT, dirs_exist_ok=True)

    # execute in test storage
    with set_working_dir(TEST_STORAGE_ROOT):
        # store dlt data in test storage (like patch_home_dir)
        with custom_environ({"DLT_DATA_DIR": get_dlt_data_dir()}):
            # save database outside of pipeline dir
            with custom_environ({"DESTINATION__DUCKDB__CREDENTIALS": "duckdb:///test_github_3.duckdb"}):
                # create virtual env with (0.3.0) before the current schema upgrade
                with Venv.create(tempfile.mkdtemp(), ["dlt[duckdb]==0.3.0"]) as venv:
                    venv._install_deps(venv.context, ["duckdb" + "==" + pkg_version("duckdb")])
                    # extract and normalize on old version but DO NOT LOAD
                    print(venv.run_script("../tests/pipeline/cases/github_pipeline/github_extract.py", "70"))
                # switch to current version and make sure the load package loads and schema migrates
                venv = Venv.restore_current()
                print(venv.run_script("../tests/pipeline/cases/github_pipeline/github_load.py"))
                duckdb_cfg = resolve_configuration(DuckDbClientConfiguration(dataset_name=GITHUB_DATASET), sections=("destination", "duckdb"))
                with DuckDbSqlClient(GITHUB_DATASET, duckdb_cfg.credentials) as client:
                    rows = client.execute_sql("SELECT * FROM issues")
                    assert len(rows) == 70
                github_schema = json.loads(test_storage.load(f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/schemas/github.schema.json"))
                # attach to existing pipeline
                pipeline = dlt.attach(GITHUB_PIPELINE_NAME, credentials=duckdb_cfg.credentials)
                # get the schema from schema storage before we sync
                github_schema = json.loads(test_storage.load(f".dlt/pipelines/{GITHUB_PIPELINE_NAME}/schemas/github.schema.json"))
                pipeline = pipeline.drop()
                pipeline.sync_destination()
                assert pipeline.default_schema.ENGINE_VERSION == 7
                # schema version does not match `dlt.attach` does not update to the right schema by itself
                assert pipeline.default_schema.stored_version_hash != github_schema["version_hash"]
