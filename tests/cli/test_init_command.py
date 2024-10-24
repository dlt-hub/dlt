import io
from copy import deepcopy
import hashlib
import os
import contextlib
from subprocess import CalledProcessError
from typing import Any, List, Tuple, Optional
from hexbytes import HexBytes
import pytest
from unittest import mock
import re
from packaging.requirements import Requirement
from typing import Dict

# import that because O3 modules cannot be unloaded
import cryptography.hazmat.bindings._rust


import dlt

from dlt.common import git
from dlt.common.configuration.providers import CONFIG_TOML, SECRETS_TOML, SecretsTomlProvider
from dlt.common.runners import Venv
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import set_working_dir


from dlt.cli import init_command, echo, utils
from dlt.cli.init_command import (
    SOURCES_MODULE_NAME,
    DEFAULT_VERIFIED_SOURCES_REPO,
    SourceConfiguration,
    utils as cli_utils,
    files_ops,
    _select_source_files,
    _list_core_sources,
    _list_template_sources,
    _list_verified_sources,
)
from dlt.cli.exceptions import CliCommandInnerException
from dlt.cli.requirements import SourceRequirements
from dlt.reflection.script_visitor import PipelineScriptVisitor
from dlt.reflection import names as n

from tests.cli.utils import (
    echo_default_choice,
    repo_dir,
    project_files,
    cloned_init_repo,
    get_repo_dir,
    get_project_files,
)
from tests.common.utils import modify_and_commit_file
from tests.utils import IMPLEMENTED_DESTINATIONS, clean_test_storage

# we hardcode the core sources here so we can check that the init script picks
# up the right source
CORE_SOURCES = ["filesystem", "rest_api", "sql_database"]

# we also hardcode all the templates here for testing
TEMPLATES = ["debug", "default", "arrow", "requests", "dataframe", "fruitshop", "github_api"]

# a few verified sources we know to exist
SOME_KNOWN_VERIFIED_SOURCES = ["chess", "google_sheets", "pipedrive"]


def get_verified_source_candidates(repo_dir: str) -> List[str]:
    sources_storage = FileStorage(os.path.join(repo_dir, SOURCES_MODULE_NAME))
    # enumerate all candidate verified sources
    return files_ops.get_sources_names(sources_storage, source_type="verified")


def test_init_command_pipeline_template(repo_dir: str, project_files: FileStorage) -> None:
    init_command.init_command("debug", "bigquery", repo_dir)
    visitor = assert_init_files(project_files, "debug_pipeline", "bigquery")
    # single resource
    assert len(visitor.known_resource_calls) == 1


def test_init_command_pipeline_default_template(repo_dir: str, project_files: FileStorage) -> None:
    init_command.init_command("some_random_name", "redshift", repo_dir)
    visitor = assert_init_files(project_files, "some_random_name_pipeline", "redshift")
    # multiple resources
    assert len(visitor.known_resource_calls) == 1


def test_default_source_file_selection() -> None:
    templates_storage = init_command._get_templates_storage()

    # try a known source, it will take the known pipeline script
    tconf = files_ops.get_template_configuration(templates_storage, "debug")
    assert tconf.dest_pipeline_script == "debug_pipeline.py"
    assert tconf.src_pipeline_script == "debug_pipeline.py"

    # random name will select the default script
    tconf = files_ops.get_template_configuration(templates_storage, "very_nice_name")
    assert tconf.dest_pipeline_script == "very_nice_name_pipeline.py"
    assert tconf.src_pipeline_script == "default_pipeline.py"


def test_init_command_new_pipeline_same_name(repo_dir: str, project_files: FileStorage) -> None:
    init_command.init_command("debug_pipeline", "bigquery", repo_dir)
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        init_command.init_command("debug_pipeline", "bigquery", repo_dir)
        _out = buf.getvalue()
    assert "already exists, exiting" in _out


def test_init_command_chess_verified_source(repo_dir: str, project_files: FileStorage) -> None:
    init_command.init_command("chess", "duckdb", repo_dir)
    assert_source_files(project_files, "chess", "duckdb", has_source_section=True)
    assert_requirements_txt(project_files, "duckdb")
    # check files hashes
    local_index = files_ops.load_verified_sources_local_index("chess")
    # chess has one file
    assert len(local_index["files"]) == 4
    # relative to "sources" folder
    assert local_index["files"][os.path.join("chess", "__init__.py")] is not None

    # delete existing pipeline if exist
    # works only if working dir is not changed by fixture
    try:
        pipeline = dlt.attach(pipeline_name="chess_players_games")
        pipeline.drop()
    except Exception as e:
        print(e)

    # now run the pipeline
    os.environ.pop(
        "DESTINATION__DUCKDB__CREDENTIALS", None
    )  # settings from local project (secrets.toml etc.)
    venv = Venv.restore_current()
    try:
        print(venv.run_script("chess_pipeline.py"))
    except CalledProcessError as cpe:
        print(cpe.stdout)
        print(cpe.stderr)
        raise


def test_list_sources(repo_dir: str) -> None:
    def check_results(items: Dict[str, SourceConfiguration]) -> None:
        for name, source in items.items():
            assert source.doc, f"{name} missing docstring"

    core_sources = _list_core_sources()
    assert set(core_sources) == set(CORE_SOURCES)
    check_results(core_sources)

    verified_sources = _list_verified_sources(DEFAULT_VERIFIED_SOURCES_REPO)
    assert set(SOME_KNOWN_VERIFIED_SOURCES).issubset(verified_sources.keys())
    check_results(verified_sources)
    assert len(verified_sources.keys()) > 10

    templates = _list_template_sources()
    assert set(templates) == set(TEMPLATES)
    check_results(templates)


def test_init_list_sources(repo_dir: str) -> None:
    # run the command and check all the sources are there
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        init_command.list_sources_command(repo_dir)
        _out = buf.getvalue()

    for source in SOME_KNOWN_VERIFIED_SOURCES + TEMPLATES + CORE_SOURCES:
        assert source in _out


def test_init_list_sources_update_warning(repo_dir: str, project_files: FileStorage) -> None:
    """Sources listed include a warning if a different dlt version is required"""
    with mock.patch.object(SourceRequirements, "current_dlt_version", return_value="0.0.1"):
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            init_command.list_sources_command(repo_dir)
            _out = buf.getvalue()

    # Check one listed source
    fb_line = [line for line in _out.splitlines() if line.startswith("facebook_ads")][0]

    pat = re.compile(r"^facebook_ads:.+\[needs update: (dlt.+)\]$")
    match = pat.match(fb_line)

    assert match
    # Try parsing the printed requiremnt string to verify it's valid
    parsed_requirement = Requirement(match.group(1))
    assert "0.0.1" not in parsed_requirement.specifier


def test_init_all_sources_together(repo_dir: str, project_files: FileStorage) -> None:
    source_candidates = [*get_verified_source_candidates(repo_dir), *CORE_SOURCES, *TEMPLATES]

    # source_candidates = [source_name for source_name in source_candidates if source_name == "salesforce"]
    for source_name in source_candidates:
        # all must install correctly
        init_command.init_command(source_name, "bigquery", repo_dir)
        # verify files
        _, secrets = assert_source_files(project_files, source_name, "bigquery")

    # requirements.txt is created from the first source and not overwritten afterwards
    assert_index_version_constraint(project_files, list(source_candidates)[0])
    # secrets should contain sections for all sources
    for source_name in source_candidates:
        assert secrets.get_value(source_name, type, None, "sources") is not None
        # must have index for this source
        assert files_ops.load_verified_sources_local_index(source_name) is not None
    # credentials for all destinations
    for destination_name in ["bigquery", "postgres", "redshift"]:
        assert secrets.get_value(destination_name, type, None, "destination") is not None


def test_init_all_sources_isolated(cloned_init_repo: FileStorage) -> None:
    repo_dir = get_repo_dir(cloned_init_repo)
    # ensure we test both sources form verified sources and core sources
    source_candidates = (
        set(get_verified_source_candidates(repo_dir)).union(set(CORE_SOURCES)).union(set(TEMPLATES))
    )
    for candidate in source_candidates:
        clean_test_storage()
        repo_dir = get_repo_dir(cloned_init_repo)
        files = get_project_files(clear_all_sources=False)
        with set_working_dir(files.storage_path):
            init_command.init_command(candidate, "bigquery", repo_dir)
            assert_source_files(files, candidate, "bigquery")
            assert_requirements_txt(files, "bigquery")
            if candidate not in CORE_SOURCES + TEMPLATES:
                assert_index_version_constraint(files, candidate)


@pytest.mark.parametrize("destination_name", IMPLEMENTED_DESTINATIONS)
def test_init_all_destinations(
    destination_name: str, project_files: FileStorage, repo_dir: str
) -> None:
    source_name = "generic"
    init_command.init_command(source_name, destination_name, repo_dir)
    assert_init_files(project_files, source_name + "_pipeline", destination_name)


def test_custom_destination_note(repo_dir: str, project_files: FileStorage):
    source_name = "generic"
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        init_command.init_command(source_name, "destination", repo_dir)
        _out = buf.getvalue()
    assert "to add a destination function that will consume your data" in _out


@pytest.mark.parametrize("omit", [True, False])
# this will break if we have new core sources that are not in verified sources anymore
@pytest.mark.parametrize("source", set(CORE_SOURCES) - {"rest_api"})
def test_omit_core_sources(
    source: str, omit: bool, project_files: FileStorage, repo_dir: str
) -> None:
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        init_command.init_command(source, "destination", repo_dir, omit_core_sources=omit)
        _out = buf.getvalue()

    # check messaging
    assert ("Omitting dlt core sources" in _out) == omit
    assert ("will no longer be copied from the" in _out) == (not omit)

    # if we omit core sources, there will be a folder with the name of the source from the verified sources repo
    assert project_files.has_folder(source) == omit
    assert (f"dlt.sources.{source}" in project_files.load(f"{source}_pipeline.py")) == (not omit)


def test_init_code_update_index_diff(repo_dir: str, project_files: FileStorage) -> None:
    sources_storage = FileStorage(os.path.join(repo_dir, SOURCES_MODULE_NAME))
    new_content = '"""New docstrings"""'
    new_content_hash = hashlib.sha3_256(bytes(new_content, encoding="ascii")).hexdigest()
    init_command.init_command("pipedrive", "duckdb", repo_dir)

    # modify existing file, no commit
    mod_file_path = os.path.join("pipedrive", "__init__.py")
    new_file_path = os.path.join("pipedrive", "new_pipedrive_X.py")
    del_file_path = os.path.join("pipedrive", "README.md")

    sources_storage.save(mod_file_path, new_content)
    sources_storage.save(new_file_path, new_content)
    sources_storage.delete(del_file_path)

    source_files = files_ops.get_verified_source_configuration(sources_storage, "pipedrive")
    remote_index = files_ops.get_remote_source_index(
        sources_storage.storage_path, source_files.files, ">=0.3.5"
    )
    assert mod_file_path in remote_index["files"]
    assert remote_index["is_dirty"] is True
    assert remote_index["files"][mod_file_path]["sha3_256"] == new_content_hash
    assert remote_index["files"][new_file_path]["sha3_256"] == new_content_hash

    # get diff
    local_index = files_ops.load_verified_sources_local_index("pipedrive")
    new, modified, deleted = files_ops.gen_index_diff(local_index, remote_index)
    # remote file entry in new
    assert new[new_file_path] == remote_index["files"][new_file_path]
    # no git sha yet
    assert new[new_file_path]["git_sha"] is None
    # remote file entry in modified
    assert modified[mod_file_path] == remote_index["files"][mod_file_path]
    # git sha didn't change (not committed)
    assert modified[mod_file_path]["git_sha"] == local_index["files"][mod_file_path]["git_sha"]
    # local entry in deleted
    assert deleted[del_file_path] == local_index["files"][del_file_path]

    # get conflicts
    conflict_modified, conflict_deleted = files_ops.find_conflict_files(
        local_index, new, modified, deleted, project_files
    )
    assert conflict_modified == []
    assert conflict_deleted == []

    # merge into local index
    modified.update(new)
    local_index = files_ops._merge_remote_index(local_index, remote_index, modified, deleted)
    assert new_file_path in local_index["files"]
    assert del_file_path not in local_index["files"]
    assert local_index["files"][mod_file_path]["sha3_256"] == new_content_hash

    # generate local conflicts
    local_content = '"""Local changes"""'
    project_files.save(new_file_path, local_content)
    project_files.save(mod_file_path, local_content)
    project_files.save(del_file_path, local_content)
    # add one more modified REMOTE file without conflict to test Merge option
    mod_file_path_2 = os.path.join("pipedrive", "new_munger_X.py")
    sources_storage.save(mod_file_path_2, local_content)
    local_index = files_ops.load_verified_sources_local_index("pipedrive")
    source_files = files_ops.get_verified_source_configuration(sources_storage, "pipedrive")
    remote_index = files_ops.get_remote_source_index(
        sources_storage.storage_path, source_files.files, ">=0.3.5"
    )
    new, modified, deleted = files_ops.gen_index_diff(local_index, remote_index)
    assert mod_file_path_2 in new
    conflict_modified, conflict_deleted = files_ops.find_conflict_files(
        local_index, new, modified, deleted, project_files
    )
    assert set(conflict_modified) == set([mod_file_path, new_file_path])
    assert set(conflict_deleted) == set([del_file_path])

    modified.update(new)
    # resolve conflicts in three different ways
    # skip option (the default)
    res, sel_modified, sel_deleted = _select_source_files(
        "pipedrive", deepcopy(modified), deepcopy(deleted), conflict_modified, conflict_deleted
    )
    # noting is written, including non-conflicting file
    assert res == "s"
    assert sel_modified == {}
    assert sel_deleted == {}
    # Apply option - local changes will be lost
    with echo.always_choose(False, "a"):
        res, sel_modified, sel_deleted = _select_source_files(
            "pipedrive", deepcopy(modified), deepcopy(deleted), conflict_modified, conflict_deleted
        )
        assert res == "a"
        assert sel_modified == modified
        assert sel_deleted == deleted
    # merge only non conflicting changes are applied
    with echo.always_choose(False, "m"):
        res, sel_modified, sel_deleted = _select_source_files(
            "pipedrive", deepcopy(modified), deepcopy(deleted), conflict_modified, conflict_deleted
        )
        assert res == "m"
        assert len(sel_modified) == 1 and mod_file_path_2 in sel_modified
        assert sel_deleted == {}

    # get rid of all the conflicts by making the local changes identical to the remote changes so they can be fast forwarded
    sources_storage.save(new_file_path, local_content)
    sources_storage.save(mod_file_path, local_content)
    project_files.delete(del_file_path)
    source_files = files_ops.get_verified_source_configuration(sources_storage, "pipedrive")
    remote_index = files_ops.get_remote_source_index(
        sources_storage.storage_path, source_files.files, ">=0.3.5"
    )
    new, modified, deleted = files_ops.gen_index_diff(local_index, remote_index)
    conflict_modified, conflict_deleted = files_ops.find_conflict_files(
        local_index, new, modified, deleted, project_files
    )
    assert conflict_modified == []
    assert conflict_deleted == []

    # generate a conflict by deleting file locally that is modified on remote
    project_files.delete(mod_file_path)
    source_files = files_ops.get_verified_source_configuration(sources_storage, "pipedrive")
    remote_index = files_ops.get_remote_source_index(
        sources_storage.storage_path, source_files.files, ">=0.3.5"
    )
    new, modified, deleted = files_ops.gen_index_diff(local_index, remote_index)
    conflict_modified, conflict_deleted = files_ops.find_conflict_files(
        local_index, new, modified, deleted, project_files
    )
    assert conflict_modified == [mod_file_path]


def test_init_code_update_no_conflict(repo_dir: str, project_files: FileStorage) -> None:
    init_command.init_command("pipedrive", "duckdb", repo_dir)
    with git.get_repo(repo_dir) as repo:
        assert git.is_clean_and_synced(repo) is True

    # a case of a new commit to existing file

    # get local index
    local_index = files_ops.load_verified_sources_local_index("pipedrive")
    # modify file in original dir
    assert "_storage" in repo_dir
    new_content = '"""New docstrings"""'
    mod_local_path = os.path.join("pipedrive", "__init__.py")
    mod_remote_path = os.path.join(SOURCES_MODULE_NAME, mod_local_path)
    assert project_files.has_file(mod_local_path)
    _, commit = modify_and_commit_file(repo_dir, mod_remote_path, content=new_content)
    # update without conflict
    init_command.init_command("pipedrive", "duckdb", repo_dir)
    # was file copied
    assert project_files.load(mod_local_path) == new_content
    with git.get_repo(repo_dir) as repo:
        assert git.is_clean_and_synced(repo) is False
        assert git.is_dirty(repo) is False
    new_local_index = files_ops.load_verified_sources_local_index("pipedrive")
    assert new_local_index["is_dirty"] is False
    assert new_local_index["last_commit_sha"] == commit.hexsha
    assert new_local_index["files"][mod_local_path]["commit_sha"] == commit.hexsha
    assert (
        new_local_index["files"][mod_local_path]["sha3_256"]
        == hashlib.sha3_256(bytes(new_content, encoding="ascii")).hexdigest()
    )
    assert (
        new_local_index["files"][mod_local_path]["git_sha"]
        != local_index["files"][mod_local_path]["git_sha"]
    )
    # all the other files must keep the old hashes
    for old_f, new_f in zip(local_index["files"].items(), new_local_index["files"].items()):
        # assert new_f[1]["commit_sha"] == commit.hexsha
        if old_f[0] != mod_local_path:
            assert old_f[1]["git_sha"] == new_f[1]["git_sha"]
            assert old_f[1]["sha3_256"] == new_f[1]["sha3_256"]

    # repeat the same: no files to update
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        init_command.init_command("pipedrive", "duckdb", repo_dir)
        _out = buf.getvalue()
    assert "No files to update, exiting" in _out

    # delete file
    repo_storage = FileStorage(repo_dir)
    repo_storage.delete(mod_remote_path)
    init_command.init_command("pipedrive", "duckdb", repo_dir)
    # file should be deleted
    assert not project_files.has_file(mod_local_path)

    # new file
    new_local_path = os.path.join("pipedrive", "__init__X.py")
    new_remote_path = os.path.join(SOURCES_MODULE_NAME, new_local_path)
    repo_storage.save(new_remote_path, new_content)
    init_command.init_command("pipedrive", "duckdb", repo_dir)
    # was file copied
    assert project_files.load(new_local_path) == new_content

    # deleting the source folder will fully reload
    project_files.delete_folder("pipedrive", recursively=True)
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        init_command.init_command("pipedrive", "duckdb", repo_dir)
        _out = buf.getvalue()
    # source was added anew
    assert "was added to your project!" in _out
    assert project_files.has_folder("pipedrive")
    # files are there
    assert project_files.load(new_local_path) == new_content


@pytest.mark.parametrize("resolution", ["s", "a", "m"])
def test_init_code_update_conflict(
    repo_dir: str, project_files: FileStorage, resolution: str
) -> None:
    init_command.init_command("pipedrive", "duckdb", repo_dir)
    repo_storage = FileStorage(repo_dir)
    mod_local_path = os.path.join("pipedrive", "__init__.py")
    mod_remote_path = os.path.join(SOURCES_MODULE_NAME, mod_local_path)
    mod_local_path_2 = os.path.join("pipedrive", "__init__X.py")
    mod_remote_path_2 = os.path.join(SOURCES_MODULE_NAME, "pipedrive", "__init__X.py")
    # change remote
    repo_storage.save(mod_remote_path, "'''VERSION 1'''")
    repo_storage.save(mod_remote_path_2, "'''VERSION 3'''")
    # change local
    project_files.save(mod_local_path, "'''VERSION 2'''")

    with echo.always_choose(False, resolution):
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            init_command.init_command("pipedrive", "duckdb", repo_dir)
            _out = buf.getvalue()

    if resolution == "s":
        assert "Skipping all incoming changes" in _out
        # local not touched
        assert project_files.load(mod_local_path) == "'''VERSION 2'''"
        assert not project_files.has_file(mod_local_path_2)
    elif resolution == "a":
        assert "Applying all incoming changes" in _out
        assert project_files.load(mod_local_path) == "'''VERSION 1'''"
        assert project_files.load(mod_local_path_2) == "'''VERSION 3'''"
    elif resolution == "m":
        assert "Merging the incoming changes" in _out
        # local changes not touched
        assert project_files.load(mod_local_path) == "'''VERSION 2'''"
        # remote changes without conflicts applied
        assert project_files.load(mod_local_path_2) == "'''VERSION 3'''"


def test_init_pyproject_toml(repo_dir: str, project_files: FileStorage) -> None:
    # add pyproject.toml to trigger dependency system
    project_files.save(cli_utils.PYPROJECT_TOML, "# toml")
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        init_command.init_command("google_sheets", "bigquery", repo_dir)
        _out = buf.getvalue()
    assert "pyproject.toml" in _out
    assert "google-api-python-client" in _out
    assert "poetry add dlt -E bigquery" in _out


def test_init_requirements_text(repo_dir: str, project_files: FileStorage) -> None:
    # add pyproject.toml to trigger dependency system
    project_files.save(cli_utils.REQUIREMENTS_TXT, "# requirements")
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        init_command.init_command("google_sheets", "bigquery", repo_dir)
        _out = buf.getvalue()
    assert "requirements.txt" in _out
    assert "google-api-python-client" in _out
    assert "pip3 install" in _out


# def test_pipeline_template_sources_in_single_file(
#     repo_dir: str, project_files: FileStorage
# ) -> None:
#     init_command.init_command("debug", "bigquery", repo_dir)
#     # SourceReference.SOURCES now contains the sources from pipeline.py which simulates loading from two places
#     with pytest.raises(CliCommandException) as cli_ex:
#         init_command.init_command("arrow", "redshift", repo_dir)
#     assert "In init scripts you must declare all sources and resources in single file." in str(
#         cli_ex.value
#     )


def test_incompatible_dlt_version_warning(repo_dir: str, project_files: FileStorage) -> None:
    with mock.patch.object(SourceRequirements, "current_dlt_version", return_value="0.1.1"):
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            init_command.init_command("facebook_ads", "bigquery", repo_dir)
            _out = buf.getvalue()

    assert (
        "WARNING: This pipeline requires a newer version of dlt than your installed version"
        " (0.1.1)."
        in _out
    )


def assert_init_files(
    project_files: FileStorage,
    pipeline_name: str,
    destination_name: str,
    dependency_destination: Optional[str] = None,
) -> PipelineScriptVisitor:
    visitor, _ = assert_common_files(project_files, pipeline_name + ".py", destination_name)
    assert not project_files.has_folder(pipeline_name)
    assert_requirements_txt(project_files, dependency_destination or destination_name)
    return visitor


def assert_requirements_txt(project_files: FileStorage, destination_name: str) -> None:
    # check requirements
    assert project_files.has_file(cli_utils.REQUIREMENTS_TXT)
    assert "dlt" in project_files.load(cli_utils.REQUIREMENTS_TXT)
    # dlt dependency specifies destination_name as extra
    source_requirements = SourceRequirements.from_string(
        project_files.load(cli_utils.REQUIREMENTS_TXT)
    )
    assert destination_name in source_requirements.dlt_requirement.extras
    # Check that atleast some version range is specified
    assert len(source_requirements.dlt_requirement.specifier) >= 1


def assert_index_version_constraint(project_files: FileStorage, source_name: str) -> None:
    # check dlt version constraint in .sources index for given source matches the one in requirements.txt
    local_index = files_ops.load_verified_sources_local_index(source_name)
    index_constraint = local_index["dlt_version_constraint"]
    assert (
        index_constraint
        == SourceRequirements.from_string(
            project_files.load(cli_utils.REQUIREMENTS_TXT)
        ).dlt_version_constraint()
    )


def assert_source_files(
    project_files: FileStorage,
    source_name: str,
    destination_name: str,
    has_source_section: bool = True,
) -> Tuple[PipelineScriptVisitor, SecretsTomlProvider]:
    visitor, secrets = assert_common_files(
        project_files, source_name + "_pipeline.py", destination_name
    )
    assert project_files.has_folder(source_name) == (source_name not in [*CORE_SOURCES, *TEMPLATES])
    source_secrets = secrets.get_value(source_name, type, None, source_name)
    if has_source_section:
        assert source_secrets is not None
    else:
        assert source_secrets is None
    local_index = files_ops.load_verified_sources_local_index(source_name)
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


def assert_common_files(
    project_files: FileStorage, pipeline_script: str, destination_name: str
) -> Tuple[PipelineScriptVisitor, SecretsTomlProvider]:
    # cwd must be project files - otherwise assert won't work
    assert os.getcwd() == project_files.storage_path
    assert project_files.has_file(utils.make_dlt_settings_path(SECRETS_TOML))
    assert project_files.has_file(utils.make_dlt_settings_path(CONFIG_TOML))
    assert project_files.has_file(".gitignore")
    assert project_files.has_file(pipeline_script)
    # inspect script
    visitor = cli_utils.parse_init_script(
        "test", project_files.load(pipeline_script), pipeline_script
    )
    # check destinations
    for args in visitor.known_calls[n.PIPELINE]:
        assert args.arguments["destination"].value == destination_name
    # load secrets
    secrets = SecretsTomlProvider(settings_dir=dlt.current.run().settings_dir)
    if destination_name not in ["duckdb", "dummy"]:
        # destination is there
        assert secrets.get_value(destination_name, type, None, "destination") is not None
    # certain values are never there
    for not_there in [
        "destination_name",
        "default_schema_name",
        "as_staging",
        "staging_config",
        "dataset_name",
    ]:
        assert secrets.get_value(not_there, type, None, "destination", destination_name)[0] is None

    return visitor, secrets
