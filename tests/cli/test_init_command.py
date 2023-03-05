import hashlib
import os
import sys
import shutil
from subprocess import CalledProcessError
from typing import Any, List, Tuple
from unittest.mock import patch
from hexbytes import HexBytes
import pytest

import dlt

from dlt.common import git
from dlt.common.configuration import make_dot_dlt_path
from dlt.common.configuration.providers import CONFIG_TOML, SECRETS_TOML, ConfigTomlProvider, SecretsTomlProvider
from dlt.common.pipeline import PipelineContext, get_default_repos_dir
from dlt.common.runners.venv import Venv
from dlt.common.storages.file_storage import FileStorage

from dlt.common.utils import set_working_dir, uniq_id


from dlt.cli import init_command, echo
from dlt.cli.init_command import PIPELINES_MODULE_NAME, utils as cli_utils, files_ops
from dlt.cli.exceptions import CliCommandException
from dlt.extract.decorators import _SOURCES
from dlt.reflection.script_visitor import PipelineScriptVisitor
from dlt.reflection import names as n
from tests.common.utils import modify_and_commit_file

from tests.pipeline.utils import patch_working_dir, drop_pipeline
from tests.utils import preserve_environ, autouse_test_storage, TEST_STORAGE_ROOT, clean_test_storage


INIT_REPO_LOCATION = "/home/rudolfix/src/python-dlt-verified-pipelines/"
PROJECT_DIR = os.path.join(TEST_STORAGE_ROOT, "project")
# INITIAL_PIPELINE_CANDIDATES: List[str] = None

@pytest.fixture(autouse=True)
def echo_default_choice() -> None:
    echo.ALWAYS_CHOOSE_DEFAULT = True
    yield
    echo.ALWAYS_CHOOSE_DEFAULT = False


@pytest.fixture(autouse=True)
def unload_modules() -> None:
    prev_modules = dict(sys.modules)
    yield
    mod_diff = set(sys.modules.keys()) - set(prev_modules.keys())
    for mod in mod_diff:
        del sys.modules[mod]


@pytest.fixture(scope="module")
def cloned_pipeline() -> FileStorage:
    return git.get_fresh_repo_files(INIT_REPO_LOCATION, get_default_repos_dir())


@pytest.fixture
def repo_dir(cloned_pipeline: FileStorage) -> str:
    return get_repo_dir(cloned_pipeline)


@pytest.fixture
def project_files() -> FileStorage:
    project_files = get_project_files()
    with set_working_dir(project_files.storage_path):
        yield project_files


def get_repo_dir(cloned_pipeline: FileStorage) -> str:
    repo_dir = os.path.abspath(os.path.join(TEST_STORAGE_ROOT, f"pipelines_repo_{uniq_id()}"))
    # copy the whole repo into TEST_STORAGE_ROOT
    shutil.copytree(cloned_pipeline.storage_path, repo_dir)
    return repo_dir


def get_project_files() -> FileStorage:
    _SOURCES.clear()
    # project dir
    return FileStorage(PROJECT_DIR, makedirs=True)


def get_pipeline_candidates(repo_dir: str) -> List[str]:
    pipelines_storage = FileStorage(os.path.join(repo_dir, PIPELINES_MODULE_NAME))
    # enumerate all candidate pipelines
    return files_ops.get_pipeline_names(pipelines_storage)


def test_init_command_template(repo_dir: str, project_files: FileStorage) -> None:
    init_command.init_command("debug_pipeline", "bigquery", False, repo_dir)
    visitor = assert_init_files(project_files, "debug_pipeline", "bigquery")
    # single resource
    assert len(visitor.known_resource_calls) == 1


def test_init_command_generic(repo_dir: str, project_files: FileStorage) -> None:
    init_command.init_command("generic_pipeline", "redshift", True, repo_dir)
    visitor = assert_init_files(project_files, "generic_pipeline", "redshift")
    # multiple resources
    assert len(visitor.known_resource_calls) > 1


def test_init_command_chess_verified_pipeline(repo_dir: str, project_files: FileStorage) -> None:
    init_command.init_command("chess", "duckdb", False, repo_dir)
    assert_pipeline_files(project_files, "chess", "duckdb", has_source_section=True)
    assert_requests_txt(project_files)
    # check files hashes
    local_index = files_ops.load_pipeline_local_index("chess")
    # chess has one file
    assert len(local_index["files"]) == 1
    # relative to "pipelines" folder
    assert local_index["files"]["chess/__init__.py"] is not None

    # now run the pipeline
    venv = Venv.restore_current()
    try:
        print(venv.run_script("chess_pipeline.py"))
    except CalledProcessError as cpe:
        print(cpe.stdout)
        print(cpe.stderr)
        raise


def test_init_list_pipelines(repo_dir: str, project_files: FileStorage) -> None:
    pipelines = init_command._list_pipelines(repo_dir)
    # a few known pipelines must be there
    known_pipelines = ["chess", "sql_database", "google_sheets", "pipedrive"]
    assert set(known_pipelines).issubset(set(pipelines.keys()))
    # check docstrings
    for k_p in known_pipelines:
        assert pipelines[k_p].doc
    # run the command
    init_command.list_pipelines_command(repo_dir)


def test_init_all_pipelines_together(repo_dir: str, project_files: FileStorage) -> None:
    pipeline_candidates = get_pipeline_candidates(repo_dir)
    for pipeline_name in pipeline_candidates:
        # all must install correctly
        init_command.init_command(pipeline_name, "bigquery", False, repo_dir)
        # verify pipeline files
        _, secrets = assert_pipeline_files(project_files, pipeline_name, "bigquery")
    # secrets should contain sections for all sources
    for pipeline_name in pipeline_candidates:
        assert secrets.get_value(pipeline_name, Any, "sources") is not None
        # must have index for pipeline
        assert files_ops.load_pipeline_local_index(pipeline_name) is not None
    # credentials for all destinations
    for destination_name in ["bigquery", "postgres", "redshift"]:
        assert secrets.get_value(destination_name, Any, "destination") is not None

    # create template on top
    init_command.init_command("debug_pipeline", "postgres", False, repo_dir)
    assert_init_files(project_files, "debug_pipeline", "postgres")
    # clear the resources otherwise sources not belonging to generic_pipeline will be found
    _SOURCES.clear()
    init_command.init_command("generic_pipeline", "redshift", True, repo_dir)
    assert_init_files(project_files, "generic_pipeline", "redshift")


def test_init_all_pipelines_isolated(cloned_pipeline: FileStorage) -> None:
    repo_dir = get_repo_dir(cloned_pipeline)
    for candidate in get_pipeline_candidates(repo_dir):
        clean_test_storage()
        repo_dir = get_repo_dir(cloned_pipeline)
        files = get_project_files()
        with set_working_dir(files.storage_path):
            init_command.init_command(candidate, "bigquery", False, repo_dir)
            assert_pipeline_files(files, candidate, "bigquery")
            assert_requests_txt(files)


def test_init_all_destinations(project_files: FileStorage) -> None:
    pass


def test_init_code_update(repo_dir: str, project_files: FileStorage) -> None:
    init_command.init_command("pipedrive", "duckdb", False, repo_dir)
    # get local index
    local_index = files_ops.load_pipeline_local_index("pipedrive")
    # modify file in original dir
    assert "_storage" in repo_dir
    new_content = '"""New docstrings"""'
    local_path = os.path.join("pipedrive", "__init__.py")
    remote_path = os.path.join(PIPELINES_MODULE_NAME, local_path)
    assert project_files.has_file(local_path)
    _, commit = modify_and_commit_file(repo_dir, remote_path, content=new_content)
    # update without conflict
    init_command.init_command("pipedrive", "duckdb", False, repo_dir)
    new_local_index = files_ops.load_pipeline_local_index("pipedrive")
    assert new_local_index["last_commit_sha"] == commit.hexsha
    assert new_local_index["files"][local_path]["commit_sha"] == commit.hexsha
    assert new_local_index["files"][local_path]["sha3_256"] == hashlib.sha3_256(bytes(new_content, encoding="ascii")).hexdigest()
    assert new_local_index["files"][local_path]["git_sha"] != local_index["files"][local_path]["git_sha"]
    # all the other files must keep the old hashesh
    for old_f, new_f in zip(local_index["files"].items(), new_local_index["files"].items()):
        # assert new_f[1]["commit_sha"] == commit.hexsha
        if old_f[0] != local_path:
            assert old_f[1]["git_sha"] == new_f[1]["git_sha"]
            assert old_f[1]["sha3_256"] == new_f[1]["sha3_256"]
    # assert_pipeline_files(project_files, "pipedrive", "duckdb")


# def test_init_code_update_conflict(project_files: FileStorage) -> None:


def test_init_pyproject_toml() -> None:
    pass


def test_init_requirements_text() -> None:
    pass


def test_pipeline_template_sources_in_single_file(repo_dir: str, project_files: FileStorage) -> None:
    init_command.init_command("debug_pipeline", "bigquery", False, repo_dir)
    # _SOURCES now contains the sources from pipeline.py which simulates loading from two places
    with pytest.raises(CliCommandException) as cli_ex:
        init_command.init_command("generic_pipeline", "redshift", True, repo_dir)
    assert "In init scripts you must declare all sources and resources in single file." in str(cli_ex.value)


def assert_init_files(project_files: FileStorage, pipeline_name: str, destination_name: str) -> PipelineScriptVisitor:
    visitor, _ = assert_common_files(project_files, pipeline_name + ".py", destination_name)
    assert not project_files.has_folder(pipeline_name)
    assert_requests_txt(project_files)
    return visitor


def assert_requests_txt(project_files: FileStorage) -> None:
    # check requirements
    assert project_files.has_file(cli_utils.REQUIREMENTS_TXT)
    assert "python-dlt" in project_files.load(cli_utils.REQUIREMENTS_TXT)


def assert_pipeline_files(project_files: FileStorage, pipeline_name: str, destination_name: str, has_source_section: bool = True) -> Tuple[PipelineScriptVisitor, SecretsTomlProvider]:
    visitor, secrets = assert_common_files(project_files, pipeline_name + "_pipeline.py", destination_name)
    assert project_files.has_folder(pipeline_name)
    source_secrets = secrets.get_value(pipeline_name, Any, pipeline_name)
    if has_source_section:
        assert source_secrets is not None
    else:
        assert source_secrets is None
    local_index = files_ops.load_pipeline_local_index(pipeline_name)
    for file_entry in local_index["files"].values():
        # all files have the newest commit (first time clone)
        assert file_entry["commit_sha"] == local_index["last_commit_sha"]
        # sha1
        assert len(HexBytes(file_entry["commit_sha"])) == 20
        # git sha
        assert len(HexBytes(file_entry["git_sha"])) == 20
        # sha3
        assert len(HexBytes(file_entry["sha3_256"])) == 32

    return visitor, secrets


def assert_common_files(project_files: FileStorage, pipeline_script: str, destination_name: str) -> Tuple[PipelineScriptVisitor, SecretsTomlProvider]:
    # cwd must be project files - otherwise assert won't work
    assert os.getcwd() == project_files.storage_path
    assert project_files.has_file(make_dot_dlt_path(SECRETS_TOML))
    assert project_files.has_file(make_dot_dlt_path(CONFIG_TOML))
    assert project_files.has_file(".gitignore")
    assert project_files.has_file(pipeline_script)
    # inspect script
    visitor = cli_utils.parse_init_script("test", project_files.load(pipeline_script), pipeline_script)
    # check destinations
    for args in visitor.known_calls[n.PIPELINE]:
        assert args.arguments["destination"].value == destination_name
    # load secrets
    secrets = SecretsTomlProvider()
    if destination_name != "duckdb":
        assert secrets.get_value(destination_name, Any, "destination") is not None
    return visitor, secrets
