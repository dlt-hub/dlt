import sys
import io
from copy import deepcopy
import hashlib
import os
import contextlib
from subprocess import CalledProcessError
from typing import List, Tuple, Optional
import pytest
from unittest import mock
import re
from packaging.requirements import Requirement
from typing import Dict
from dlt.common.libs.hexbytes import HexBytes

# import that because O3 modules cannot be unloaded
import cryptography.hazmat.bindings._rust


import dlt
from dlt.common.libs import git
from dlt.common.configuration.providers import CONFIG_TOML, SECRETS_TOML, SecretsTomlProvider
from dlt.common.runners import Venv
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import uniq_id


from dlt._workspace.cli import (
    _init_command,
    echo,
    utils,
    DEFAULT_VERIFIED_SOURCES_REPO,
)
from dlt._workspace.cli._init_command import (
    SOURCES_MODULE_NAME,
    SourceConfiguration,
    utils as cli_utils,
    files_ops,
    _select_source_files,
    _list_core_sources,
    _list_template_sources,
    _list_verified_sources,
)
from dlt._workspace.cli._ai_command import SUPPORTED_IDES
from dlt._workspace.cli.requirements import SourceRequirements
from dlt.reflection.script_visitor import PipelineScriptVisitor
from dlt.reflection import names as n
from dlt._workspace.cli.config_toml_writer import TYPE_EXAMPLES
from dlt._workspace.cli._pipeline_files import TSourceType

from tests.workspace.cli.utils import (
    auto_echo_default_choice,
    repo_dir,
    vibe_repo_dir,
    workspace_files,
    cloned_init_repo,
    cloned_init_vibe_repo,
    get_repo_dir,
    get_workspace_files,
)
from tests.common.utils import modify_and_commit_file
from tests.utils import IMPLEMENTED_DESTINATIONS
from tests.workspace.utils import restore_clean_workspace

# we hardcode the core sources here so we can check that the init script picks
# up the right source
CORE_SOURCES_CONFIG = {
    "rest_api": {"requires_extra": False},
    "sql_database": {"requires_extra": True},
    "filesystem": {"requires_extra": True},
}
CORE_SOURCES = list(CORE_SOURCES_CONFIG.keys())

# we also hardcode all the templates here for testing
TEMPLATES = [
    "debug",
    "default",
    "arrow",
    "requests",
    "dataframe",
    "fruitshop",
    "github_api",
    "vibe_rest_api",
]

# a few verified sources we know to exist
SOME_KNOWN_VERIFIED_SOURCES = ["chess", "google_sheets", "pipedrive"]

from tests.utils import (
    auto_unload_modules,
)


@pytest.fixture(autouse=True)
def auto_unload_core_sources(auto_unload_modules) -> None:
    """Unload core sources so all init tests will pass"""
    sys.modules.pop("dlt.sources.rest_api", None)
    sys.modules.pop("dlt.sources.sql_database", None)
    sys.modules.pop("dlt.sources.filesystem", None)


def get_source_candidates(repo_dir: str, source_type: TSourceType = "verified") -> List[str]:
    # vibe sources are in the root folder, so no module name
    if source_type == "vibe":
        sources_storage = FileStorage(repo_dir)
    else:
        sources_storage = FileStorage(os.path.join(repo_dir, SOURCES_MODULE_NAME))
    # enumerate all candidate sources
    return files_ops.get_sources_names(sources_storage, source_type=source_type)


def test_init_command_pipeline_template(repo_dir: str, workspace_files: FileStorage) -> None:
    _init_command.init_command("debug", "bigquery", repo_dir)
    visitor = assert_init_files(workspace_files, "debug_pipeline", "bigquery")
    # single resource
    assert len(visitor.known_resource_calls) == 1


def test_init_command_pipeline_default_template(
    repo_dir: str, workspace_files: FileStorage
) -> None:
    _init_command.init_command("some_random_name", "redshift", repo_dir)
    visitor = assert_init_files(workspace_files, "some_random_name_pipeline", "redshift")
    # multiple resources
    assert len(visitor.known_resource_calls) == 1


def test_default_source_file_selection() -> None:
    templates_storage = files_ops.get_single_file_templates_storage()

    # try a known source, it will take the known pipeline script
    tconf = files_ops.get_template_configuration(templates_storage, "debug", "debug")
    assert tconf.dest_pipeline_script == "debug_pipeline.py"
    assert tconf.src_pipeline_script == "debug_pipeline.py"

    # random name will select the default script
    tconf = files_ops.get_template_configuration(
        templates_storage, "very_nice_name", "very_nice_name"
    )
    assert tconf.dest_pipeline_script == "very_nice_name_pipeline.py"
    assert tconf.src_pipeline_script == "default_pipeline.py"

    # you can set the dest script name for existing scripts
    tconf = files_ops.get_template_configuration(templates_storage, "vibe_rest_api", "notion")
    assert tconf.dest_pipeline_script == "notion_pipeline.py"
    assert tconf.src_pipeline_script == "vibe_rest_api_pipeline.py"


def test_init_command_new_pipeline_same_name(repo_dir: str, workspace_files: FileStorage) -> None:
    _init_command.init_command("debug_pipeline", "bigquery", repo_dir)
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _init_command.init_command("debug_pipeline", "bigquery", repo_dir)
        _out = buf.getvalue()
    assert "already exists, exiting" in _out


def test_init_command_chess_verified_source(repo_dir: str, workspace_files: FileStorage) -> None:
    _init_command.init_command("chess", "duckdb", repo_dir)
    assert_source_files(workspace_files, "chess", "duckdb", has_source_section=True)
    assert_requirements_txt(workspace_files, "duckdb")
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
        _init_command.list_sources_command(repo_dir)
        _out = buf.getvalue()

    for source in SOME_KNOWN_VERIFIED_SOURCES + TEMPLATES + CORE_SOURCES:
        assert source in _out


def test_init_list_destinations() -> None:
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _init_command.list_destinations_command()
        _out = buf.getvalue()

    for destination in IMPLEMENTED_DESTINATIONS:
        assert destination in _out


@pytest.mark.parametrize(
    "source_name",
    [name for name in CORE_SOURCES_CONFIG if CORE_SOURCES_CONFIG[name]["requires_extra"]],
)
def test_init_command_core_source_requirements_with_extras(
    source_name: str, repo_dir: str, workspace_files: FileStorage
) -> None:
    _init_command.init_command(source_name, "duckdb", repo_dir)
    source_requirements = SourceRequirements.from_string(
        workspace_files.load(cli_utils.REQUIREMENTS_TXT)
    )
    canonical_name = source_name.replace("_", "-")
    assert canonical_name in source_requirements.dlt_requirement.extras


@pytest.mark.parametrize(
    "source_name",
    [name for name in CORE_SOURCES_CONFIG if not CORE_SOURCES_CONFIG[name]["requires_extra"]],
)
def test_init_command_core_source_requirements_without_extras(
    source_name: str, repo_dir: str, workspace_files: FileStorage
) -> None:
    _init_command.init_command(source_name, "duckdb", repo_dir)
    source_requirements = SourceRequirements.from_string(
        workspace_files.load(cli_utils.REQUIREMENTS_TXT)
    )
    assert source_requirements.dlt_requirement.extras == {
        "duckdb"
    }, "Only duckdb should be in extras"


def test_init_list_sources_update_warning(repo_dir: str, workspace_files: FileStorage) -> None:
    """Sources listed include a warning if a different dlt version is required"""
    with mock.patch.object(SourceRequirements, "current_dlt_version", return_value="0.0.1"):
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            _init_command.list_sources_command(repo_dir)
            _out = buf.getvalue()

    # Check one listed source
    fb_line = [line for line in _out.splitlines() if line.startswith("facebook_ads")][0]

    pat = re.compile(r"^facebook_ads:.+\[needs update: (dlt.+)\]$")
    match = pat.match(fb_line)

    assert match
    # Try parsing the printed requiremnt string to verify it's valid
    parsed_requirement = Requirement(match.group(1))
    assert "0.0.1" not in parsed_requirement.specifier


def test_init_all_sources_together(repo_dir: str, workspace_files: FileStorage) -> None:
    source_candidates = [*get_source_candidates(repo_dir), *CORE_SOURCES, *TEMPLATES]

    # source_candidates = [source_name for source_name in source_candidates if source_name == "salesforce"]
    for source_name in source_candidates:
        # all must install correctly
        _init_command.init_command(source_name, "bigquery", repo_dir)
        # verify files
        _, secrets = assert_source_files(workspace_files, source_name, "bigquery")

    # requirements.txt is created from the first source and not overwritten afterwards
    assert_index_version_constraint(workspace_files, list(source_candidates)[0])
    # secrets should contain sections for all sources
    for source_name in source_candidates:
        assert secrets.get_value(source_name, type, None, "sources") is not None
        # must have index for this source
        assert files_ops.load_verified_sources_local_index(source_name) is not None
    # credentials for all destinations
    for destination_name in ["bigquery", "postgres", "redshift"]:
        assert secrets.get_value(destination_name, type, None, "destination") is not None


def test_init_all_sources_isolated(cloned_init_repo: FileStorage) -> None:
    repo_dir = get_repo_dir(cloned_init_repo, f"verified_sources_repo_{uniq_id()}")
    # ensure we test both sources form verified sources and core sources
    source_candidates = (
        set(get_source_candidates(repo_dir)).union(set(CORE_SOURCES)).union(set(TEMPLATES))
    )
    for candidate in source_candidates:
        # this is not really changing chdir - we are setting the same folder on a new inode
        os.chdir(restore_clean_workspace("empty"))
        repo_dir = get_repo_dir(cloned_init_repo, f"verified_sources_repo_{uniq_id()}")
        files = get_workspace_files(clear_all_sources=False)
        _init_command.init_command(candidate, "bigquery", repo_dir)
        assert_source_files(files, candidate, "bigquery")
        assert_requirements_txt(files, "bigquery")
        if candidate not in CORE_SOURCES + TEMPLATES:
            assert_index_version_constraint(files, candidate)


def test_init_core_sources_ejected(cloned_init_repo: FileStorage) -> None:
    repo_dir = get_repo_dir(cloned_init_repo, f"verified_sources_repo_{uniq_id()}")
    # ensure we test both sources form verified sources and core sources
    source_candidates = set(CORE_SOURCES)
    for candidate in source_candidates:
        # this is not really changing chdir - we are setting the same folder on a new inode
        os.chdir(restore_clean_workspace("empty"))
        repo_dir = get_repo_dir(cloned_init_repo, f"verified_sources_repo_{uniq_id()}")
        files = get_workspace_files(clear_all_sources=False)
        _init_command.init_command(candidate, "bigquery", repo_dir, eject_source=True)
        assert_requirements_txt(files, "bigquery")
        # check if files copied
        assert files.has_folder(candidate)


def test_init_writes_example_config_placeholders(
    repo_dir: str, workspace_files: FileStorage
) -> None:
    _init_command.init_command("filesystem", "bigquery", repo_dir)
    # check that written secret of type string was replaced with correct placeholder value
    secrets = SecretsTomlProvider(settings_dir=dlt.current.run_context().settings_dir)
    access_key_value, _ = secrets.get_value(
        "aws_access_key_id", str, "", "sources", "filesystem", "credentials"
    )
    assert access_key_value == TYPE_EXAMPLES["text"]


@pytest.mark.parametrize("destination_name", IMPLEMENTED_DESTINATIONS)
def test_init_all_destinations(
    destination_name: str, workspace_files: FileStorage, repo_dir: str
) -> None:
    source_name = "generic"
    _init_command.init_command(source_name, destination_name, repo_dir)
    assert_init_files(workspace_files, source_name + "_pipeline", destination_name)


def test_custom_destination_note(repo_dir: str, workspace_files: FileStorage):
    source_name = "generic"
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _init_command.init_command(source_name, "destination", repo_dir)
        _out = buf.getvalue()
    assert "to add a destination function that will consume your data" in _out


def test_init_code_update_index_diff(repo_dir: str, workspace_files: FileStorage) -> None:
    sources_storage = FileStorage(os.path.join(repo_dir, SOURCES_MODULE_NAME))
    new_content = '"""New docstrings"""'
    new_content_hash = hashlib.sha3_256(bytes(new_content, encoding="ascii")).hexdigest()
    _init_command.init_command("pipedrive", "duckdb", repo_dir)

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
        local_index, new, modified, deleted, workspace_files
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
    workspace_files.save(new_file_path, local_content)
    workspace_files.save(mod_file_path, local_content)
    workspace_files.save(del_file_path, local_content)
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
        local_index, new, modified, deleted, workspace_files
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
    workspace_files.delete(del_file_path)
    source_files = files_ops.get_verified_source_configuration(sources_storage, "pipedrive")
    remote_index = files_ops.get_remote_source_index(
        sources_storage.storage_path, source_files.files, ">=0.3.5"
    )
    new, modified, deleted = files_ops.gen_index_diff(local_index, remote_index)
    conflict_modified, conflict_deleted = files_ops.find_conflict_files(
        local_index, new, modified, deleted, workspace_files
    )
    assert conflict_modified == []
    assert conflict_deleted == []

    # generate a conflict by deleting file locally that is modified on remote
    workspace_files.delete(mod_file_path)
    source_files = files_ops.get_verified_source_configuration(sources_storage, "pipedrive")
    remote_index = files_ops.get_remote_source_index(
        sources_storage.storage_path, source_files.files, ">=0.3.5"
    )
    new, modified, deleted = files_ops.gen_index_diff(local_index, remote_index)
    conflict_modified, conflict_deleted = files_ops.find_conflict_files(
        local_index, new, modified, deleted, workspace_files
    )
    assert conflict_modified == [mod_file_path]


def test_init_code_update_no_conflict(repo_dir: str, workspace_files: FileStorage) -> None:
    _init_command.init_command("pipedrive", "duckdb", repo_dir)
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
    assert workspace_files.has_file(mod_local_path)
    _, commit = modify_and_commit_file(repo_dir, mod_remote_path, content=new_content)
    # update without conflict
    _init_command.init_command("pipedrive", "duckdb", repo_dir)
    # was file copied
    assert workspace_files.load(mod_local_path) == new_content
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
        _init_command.init_command("pipedrive", "duckdb", repo_dir)
        _out = buf.getvalue()
    assert "No files to update, exiting" in _out

    # delete file
    repo_storage = FileStorage(repo_dir)
    repo_storage.delete(mod_remote_path)
    _init_command.init_command("pipedrive", "duckdb", repo_dir)
    # file should be deleted
    assert not workspace_files.has_file(mod_local_path)

    # new file
    new_local_path = os.path.join("pipedrive", "__init__X.py")
    new_remote_path = os.path.join(SOURCES_MODULE_NAME, new_local_path)
    repo_storage.save(new_remote_path, new_content)
    _init_command.init_command("pipedrive", "duckdb", repo_dir)
    # was file copied
    assert workspace_files.load(new_local_path) == new_content

    # deleting the source folder will fully reload
    workspace_files.delete_folder("pipedrive", recursively=True)
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _init_command.init_command("pipedrive", "duckdb", repo_dir)
        _out = buf.getvalue()
    # source was added anew
    assert "was added to your project!" in _out
    assert workspace_files.has_folder("pipedrive")
    # files are there
    assert workspace_files.load(new_local_path) == new_content


@pytest.mark.parametrize("resolution", ["s", "a", "m"])
def test_init_code_update_conflict(
    repo_dir: str, workspace_files: FileStorage, resolution: str
) -> None:
    _init_command.init_command("pipedrive", "duckdb", repo_dir)
    repo_storage = FileStorage(repo_dir)
    mod_local_path = os.path.join("pipedrive", "__init__.py")
    mod_remote_path = os.path.join(SOURCES_MODULE_NAME, mod_local_path)
    mod_local_path_2 = os.path.join("pipedrive", "__init__X.py")
    mod_remote_path_2 = os.path.join(SOURCES_MODULE_NAME, "pipedrive", "__init__X.py")
    # change remote
    repo_storage.save(mod_remote_path, "'''VERSION 1'''")
    repo_storage.save(mod_remote_path_2, "'''VERSION 3'''")
    # change local
    workspace_files.save(mod_local_path, "'''VERSION 2'''")

    with echo.always_choose(False, resolution):
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            _init_command.init_command("pipedrive", "duckdb", repo_dir)
            _out = buf.getvalue()

    if resolution == "s":
        assert "Skipping all incoming changes" in _out
        # local not touched
        assert workspace_files.load(mod_local_path) == "'''VERSION 2'''"
        assert not workspace_files.has_file(mod_local_path_2)
    elif resolution == "a":
        assert "Applying all incoming changes" in _out
        assert workspace_files.load(mod_local_path) == "'''VERSION 1'''"
        assert workspace_files.load(mod_local_path_2) == "'''VERSION 3'''"
    elif resolution == "m":
        assert "Merging the incoming changes" in _out
        # local changes not touched
        assert workspace_files.load(mod_local_path) == "'''VERSION 2'''"
        # remote changes without conflicts applied
        assert workspace_files.load(mod_local_path_2) == "'''VERSION 3'''"


def test_init_pyproject_toml(repo_dir: str, workspace_files: FileStorage) -> None:
    # add pyproject.toml to trigger dependency system
    workspace_files.save(cli_utils.PYPROJECT_TOML, "# toml")
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _init_command.init_command("google_sheets", "bigquery", repo_dir)
        _out = buf.getvalue()
    assert "pyproject.toml" in _out
    assert "google-api-python-client" in _out
    assert "poetry add dlt -E bigquery" in _out


def test_init_requirements_text(repo_dir: str, workspace_files: FileStorage) -> None:
    # add pyproject.toml to trigger dependency system
    workspace_files.save(cli_utils.REQUIREMENTS_TXT, "# requirements")
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _init_command.init_command("google_sheets", "bigquery", repo_dir)
        _out = buf.getvalue()
    assert "requirements.txt" in _out
    assert "google-api-python-client" in _out
    assert "pip3 install" in _out


# def test_pipeline_template_sources_in_single_file(
#     repo_dir: str, workspace_files: FileStorage
# ) -> None:
#     _init_command.init_command("debug", "bigquery", repo_dir)
#     # SourceReference.SOURCES now contains the sources from pipeline.py which simulates loading from two places
#     with pytest.raises(CliCommandException) as cli_ex:
#         _init_command.init_command("arrow", "redshift", repo_dir)
#     assert "In init scripts you must declare all sources and resources in single file." in str(
#         cli_ex.value
#     )


def test_incompatible_dlt_version_warning(repo_dir: str, workspace_files: FileStorage) -> None:
    with mock.patch.object(SourceRequirements, "current_dlt_version", return_value="0.1.1"):
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            _init_command.init_command("facebook_ads", "bigquery", repo_dir)
            _out = buf.getvalue()

    assert (
        "WARNING: This pipeline requires a newer version of dlt than your installed version"
        " (0.1.1)."
        in _out
    )


@pytest.mark.parametrize(
    "ide_choice",
    SUPPORTED_IDES,
)
def test_init_vibe_source_editor_choice_ux(
    ide_choice: str, vibe_repo_dir: str, workspace_files: FileStorage
) -> None:
    # Second yes/no prompt also receives the ide_choice, but it doesn't matter
    with echo.always_choose(False, ide_choice):
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            _init_command.init_command("dlthub:github", "duckdb", vibe_repo_dir)
            _out = buf.getvalue()

    assert "dlt will generate useful project rules tailored to your assistant/IDE." in _out
    assert f"adding {ide_choice} rules, code snippets and docs" in _out
    assert "file(s) supporting github were copied:\ngithub-docs.yaml\n" in _out


def test_init_all_vibe_sources_together(vibe_repo_dir: str, workspace_files: FileStorage) -> None:
    # we test 20 hardcoded sources, use this to get all sources instead
    # vibe_source_candidates = [*get_source_candidates(vibe_repo_dir, source_type="vibe")]
    random_vibez = [
        "news_api",
        "alpaca",
        "robin",
        "kwanko",
        "powerlink",
        "fulcrum_data_management",
        "mysql_instance",
        "talkdesk_reports",
        "insightly_crm",
        "google_drive",
        "coalesce",
        "jobnimbus",
        "piwik_pro",
        "perplexity_ai",
        "maileon",
        "wrike_project_management",
        "rocketreach",
        "wordpress_site",
        "deepinfra",
        "no_crm_io",
    ]

    for source_name in random_vibez:
        _init_command.init_command(f"dlthub:{source_name}", "bigquery", vibe_repo_dir)
        # all must install correctly
        _, secrets = assert_source_files(
            workspace_files, source_name, "bigquery", has_source_section=True, is_vibe_source=True
        )

    for source_name in random_vibez:
        assert secrets.get_value(source_name, type, None, "sources") is not None

    # credentials for all destinations
    for destination_name in ["bigquery", "postgres", "redshift"]:
        assert secrets.get_value(destination_name, type, None, "destination") is not None


def assert_init_files(
    workspace_files: FileStorage,
    pipeline_name: str,
    destination_name: str,
    dependency_destination: Optional[str] = None,
) -> PipelineScriptVisitor:
    visitor, _ = assert_common_files(workspace_files, pipeline_name + ".py", destination_name)
    assert not workspace_files.has_folder(pipeline_name)
    assert_requirements_txt(workspace_files, dependency_destination or destination_name)
    return visitor


def assert_requirements_txt(workspace_files: FileStorage, destination_name: str) -> None:
    # check requirements
    assert workspace_files.has_file(cli_utils.REQUIREMENTS_TXT)
    assert "dlt" in workspace_files.load(cli_utils.REQUIREMENTS_TXT)
    # dlt dependency specifies destination_name as extra
    source_requirements = SourceRequirements.from_string(
        workspace_files.load(cli_utils.REQUIREMENTS_TXT)
    )
    assert destination_name in source_requirements.dlt_requirement.extras
    # Check that at least some version range is specified
    assert len(source_requirements.dlt_requirement.specifier) >= 1


def assert_index_version_constraint(workspace_files: FileStorage, source_name: str) -> None:
    # check dlt version constraint in .sources index for given source matches the one in requirements.txt
    local_index = files_ops.load_verified_sources_local_index(source_name)
    index_constraint = local_index["dlt_version_constraint"]
    assert (
        index_constraint
        == SourceRequirements.from_string(
            workspace_files.load(cli_utils.REQUIREMENTS_TXT)
        ).dlt_version_constraint()
    )


def assert_source_files(
    workspace_files: FileStorage,
    source_name: str,
    destination_name: str,
    has_source_section: bool = True,
    is_vibe_source: bool = False,
) -> Tuple[PipelineScriptVisitor, SecretsTomlProvider]:
    visitor, secrets = assert_common_files(
        workspace_files, source_name + "_pipeline.py", destination_name
    )
    assert workspace_files.has_folder(source_name) == (
        source_name not in [*CORE_SOURCES, *TEMPLATES] and not is_vibe_source
    )
    source_secrets = secrets.get_value(source_name, type, None, source_name)
    if has_source_section:
        assert source_secrets is not None
    else:
        assert source_secrets is None
    # no .sources files for vibe sources
    if not is_vibe_source:
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
    else:
        # Ensure the yaml file is there for vibe sources
        assert workspace_files.has_file(f"{source_name}-docs.yaml")
        # Ensure rules are there (cursor by default)
        assert workspace_files.has_folder(".cursor")

    return visitor, secrets


def assert_common_files(
    workspace_files: FileStorage, pipeline_script: str, destination_name: str
) -> Tuple[PipelineScriptVisitor, SecretsTomlProvider]:
    # cwd must be project files - otherwise assert won't work
    assert os.getcwd() == workspace_files.storage_path
    assert workspace_files.has_file(utils.make_dlt_settings_path(SECRETS_TOML))
    assert workspace_files.has_file(utils.make_dlt_settings_path(CONFIG_TOML))
    assert workspace_files.has_file(".gitignore")
    assert workspace_files.has_file(pipeline_script)
    # inspect script
    visitor = cli_utils.parse_init_script(
        "test", workspace_files.load(pipeline_script), pipeline_script
    )
    # check destinations
    for args in visitor.known_calls[n.PIPELINE]:
        assert args.arguments["destination"].value == destination_name
    # load secrets
    secrets = SecretsTomlProvider(settings_dir=dlt.current.run_context().settings_dir)
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
