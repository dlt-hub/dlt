import fnmatch
import hashlib
import os
import yaml
import posixpath
from pathlib import Path
from typing import Dict, NamedTuple, Sequence, Tuple, List, Literal
from dlt.cli.exceptions import VerifiedSourceRepoError

from dlt.common import git
from dlt.common.storages import FileStorage
from dlt.common.typing import TypedDict

from dlt.common.reflection.utils import get_module_docstring

from dlt.cli import utils
from dlt.cli.requirements import SourceRequirements

TSourceType = Literal["core", "verified", "template", "vibe"]

SOURCES_INIT_INFO_ENGINE_VERSION = 1

SOURCES_MODULE_NAME = "sources"
CORE_SOURCE_TEMPLATE_MODULE_NAME = "_core_source_templates"
SINGLE_FILE_TEMPLATE_MODULE_NAME = "_single_file_templates"

SOURCES_INIT_INFO_FILE = ".sources"
IGNORE_FILES = ["*.py[cod]", "*$py.class", "__pycache__", "py.typed", "requirements.txt"]
IGNORE_VERIFIED_SOURCES = [".*", "_*"]
IGNORE_CORE_SOURCES = [
    ".*",
    "_*",
    "helpers",
    SINGLE_FILE_TEMPLATE_MODULE_NAME,
    CORE_SOURCE_TEMPLATE_MODULE_NAME,
]
PIPELINE_FILE_SUFFIX = "_pipeline.py"
# hardcode default template files here
TEMPLATE_FILES = [".gitignore", ".dlt/config.toml"]
DEFAULT_PIPELINE_TEMPLATE = "default_pipeline.py"


class SourceConfiguration(NamedTuple):
    source_type: TSourceType
    source_module_prefix: str
    storage: FileStorage
    src_pipeline_script: str
    dest_pipeline_script: str
    files: List[str]
    requirements: SourceRequirements
    doc: str
    is_default_template: bool


class TVerifiedSourceFileEntry(TypedDict):
    commit_sha: str
    git_sha: str
    sha3_256: str


class TVerifiedSourceFileIndex(TypedDict):
    is_dirty: bool
    last_commit_sha: str
    last_commit_timestamp: str
    files: Dict[str, TVerifiedSourceFileEntry]
    dlt_version_constraint: str


class TVerifiedSourcesFileIndex(TypedDict):
    engine_version: int
    sources: Dict[str, TVerifiedSourceFileIndex]


def _save_dot_sources(index: TVerifiedSourcesFileIndex) -> None:
    with open(utils.make_dlt_settings_path(SOURCES_INIT_INFO_FILE), "w", encoding="utf-8") as f:
        yaml.dump(index, f, allow_unicode=True, default_flow_style=False, sort_keys=False)


def _load_dot_sources() -> TVerifiedSourcesFileIndex:
    try:
        with open(utils.make_dlt_settings_path(SOURCES_INIT_INFO_FILE), "r", encoding="utf-8") as f:
            index: TVerifiedSourcesFileIndex = yaml.safe_load(f)
            if not index:
                raise FileNotFoundError(SOURCES_INIT_INFO_FILE)
            return index
    except FileNotFoundError:
        return {"engine_version": SOURCES_INIT_INFO_ENGINE_VERSION, "sources": {}}


def _merge_remote_index(
    local_index: TVerifiedSourceFileIndex,
    remote_index: TVerifiedSourceFileIndex,
    remote_modified: Dict[str, TVerifiedSourceFileEntry],
    remote_deleted: Dict[str, TVerifiedSourceFileEntry],
) -> TVerifiedSourceFileIndex:
    # update all modified files
    local_index["files"].update(remote_modified)
    # delete all deleted
    for deleted in remote_deleted:
        del local_index["files"][deleted]
    # update global info
    local_index["is_dirty"] = remote_index["is_dirty"]
    local_index["last_commit_sha"] = remote_index["last_commit_sha"]
    local_index["last_commit_timestamp"] = remote_index["last_commit_timestamp"]
    local_index["dlt_version_constraint"] = remote_index["dlt_version_constraint"]

    return local_index


def load_verified_sources_local_index(source_name: str) -> TVerifiedSourceFileIndex:
    return _load_dot_sources()["sources"].get(
        source_name,
        {
            "is_dirty": False,
            "last_commit_sha": None,
            "last_commit_timestamp": None,
            "files": {},
            "dlt_version_constraint": ">=0.1.0",
        },
    )


def save_verified_source_local_index(
    source_name: str,
    remote_index: TVerifiedSourceFileIndex,
    remote_modified: Dict[str, TVerifiedSourceFileEntry],
    remote_deleted: Dict[str, TVerifiedSourceFileEntry],
) -> None:
    all_sources = _load_dot_sources()
    local_index = all_sources["sources"].setdefault(source_name, remote_index)
    _merge_remote_index(local_index, remote_index, remote_modified, remote_deleted)
    _save_dot_sources(all_sources)


def get_remote_source_index(
    repo_path: str, files: Sequence[str], dlt_version_constraint: str
) -> TVerifiedSourceFileIndex:
    with git.get_repo(repo_path) as repo:
        tree = repo.tree()
        commit_sha = repo.head.commit.hexsha
        files_sha: Dict[str, TVerifiedSourceFileEntry] = {}
        for file in files:
            posix_file = os.path.join(repo_path, file)
            posix_file = os.path.relpath(posix_file, repo.working_dir)
            posix_file = posixpath.join(*Path(posix_file).parts)
            try:
                blob_sha3 = tree.join(posix_file).hexsha
            except KeyError:
                # if directory is dirty and we do not have git sha
                blob_sha3 = None

            with open(os.path.join(repo_path, file), "rb") as f:
                file_blob = f.read()
            files_sha[file] = {
                "commit_sha": commit_sha,
                "git_sha": blob_sha3,
                "sha3_256": hashlib.sha3_256(file_blob).hexdigest(),
            }

        return {
            "is_dirty": git.is_dirty(repo),
            "last_commit_sha": commit_sha,
            "last_commit_timestamp": repo.head.commit.committed_datetime.isoformat(),
            "files": files_sha,
            "dlt_version_constraint": dlt_version_constraint,
        }


def get_sources_names(sources_storage: FileStorage, source_type: TSourceType) -> List[str]:
    candidates: List[str] = []

    # for vibe sources, we just find all directories, except for hidden ones (.git)
    if source_type == "vibe":
        candidates = [
            name
            for name in sources_storage.list_folder_dirs(".", to_root=False)
            if not name.startswith(".")
        ]
    # for the templates we just find all the filenames
    elif source_type == "template":
        for name in sources_storage.list_folder_files(".", to_root=False):
            if name.endswith(PIPELINE_FILE_SUFFIX):
                candidates.append(name.replace(PIPELINE_FILE_SUFFIX, ""))
    else:
        ignore_cases = IGNORE_VERIFIED_SOURCES if source_type == "verified" else IGNORE_CORE_SOURCES
        for name in [
            n
            for n in sources_storage.list_folder_dirs(".", to_root=False)
            if not any(fnmatch.fnmatch(n, ignore) for ignore in ignore_cases)
        ]:
            # must contain at least one valid python script
            if any(
                f.endswith(".py") for f in sources_storage.list_folder_files(name, to_root=False)
            ):
                candidates.append(name)

    candidates.sort()
    return candidates


def _get_docstring_for_module(sources_storage: FileStorage, source_name: str) -> str:
    # read the docs
    init_py = os.path.join(source_name, utils.MODULE_INIT)
    docstring: str = ""
    if sources_storage.has_file(init_py):
        docstring = get_module_docstring(sources_storage.load(init_py))
        if docstring:
            docstring = docstring.splitlines()[0]
    return docstring


def get_template_configuration(
    sources_storage: FileStorage, source_name: str, display_source_name: str
) -> SourceConfiguration:
    destination_pipeline_file_name = display_source_name + PIPELINE_FILE_SUFFIX
    source_pipeline_file_name = source_name + PIPELINE_FILE_SUFFIX

    if not sources_storage.has_file(source_pipeline_file_name):
        source_pipeline_file_name = DEFAULT_PIPELINE_TEMPLATE

    docstring = get_module_docstring(sources_storage.load(source_pipeline_file_name))
    if docstring:
        docstring = docstring.splitlines()[0]
    return SourceConfiguration(
        "template",
        source_pipeline_file_name.replace("pipeline.py", ""),
        sources_storage,
        source_pipeline_file_name,
        destination_pipeline_file_name,
        [],
        SourceRequirements([]),
        docstring,
        source_pipeline_file_name == DEFAULT_PIPELINE_TEMPLATE,
    )


def _get_source_files(sources_storage: FileStorage, source_name: str) -> List[str]:
    """Get all files that belong to source `source_name`"""
    files: List[str] = []
    for root, subdirs, _files in os.walk(sources_storage.make_full_path(source_name)):
        # filter unwanted files
        for subdir in list(subdirs):
            if any(fnmatch.fnmatch(subdir, ignore) for ignore in IGNORE_FILES):
                subdirs.remove(subdir)
        rel_root = sources_storage.to_relative_path(root)
        files.extend(
            [
                os.path.join(rel_root, file)
                for file in _files
                if all(not fnmatch.fnmatch(file, ignore) for ignore in IGNORE_FILES)
            ]
        )
    return files


def get_core_source_configuration(
    sources_storage: FileStorage, source_name: str, eject_source: bool
) -> SourceConfiguration:
    src_pipeline_file = CORE_SOURCE_TEMPLATE_MODULE_NAME + "/" + source_name + PIPELINE_FILE_SUFFIX
    dest_pipeline_file = source_name + PIPELINE_FILE_SUFFIX
    files: List[str] = _get_source_files(sources_storage, source_name) if eject_source else []

    return SourceConfiguration(
        "core",
        "dlt.sources." + source_name,
        sources_storage,
        src_pipeline_file,
        dest_pipeline_file,
        files,
        SourceRequirements([]),
        _get_docstring_for_module(sources_storage, source_name),
        False,
    )


def get_verified_source_configuration(
    sources_storage: FileStorage, source_name: str
) -> SourceConfiguration:
    if not sources_storage.has_folder(source_name):
        raise VerifiedSourceRepoError(
            f"Verified source {source_name} could not be found in the repository", source_name
        )
    # find example script
    example_script = f"{source_name}{PIPELINE_FILE_SUFFIX}"
    if not sources_storage.has_file(example_script):
        raise VerifiedSourceRepoError(
            f"Pipeline example script {example_script} could not be found in the repository",
            source_name,
        )
    files = _get_source_files(sources_storage, source_name)
    # read requirements
    requirements_path = os.path.join(source_name, utils.REQUIREMENTS_TXT)
    if sources_storage.has_file(requirements_path):
        requirements = SourceRequirements.from_string(sources_storage.load(requirements_path))
    else:
        requirements = SourceRequirements([])
    # find requirements
    return SourceConfiguration(
        "verified",
        source_name,
        sources_storage,
        example_script,
        example_script,
        files,
        requirements,
        _get_docstring_for_module(sources_storage, source_name),
        False,
    )


def gen_index_diff(
    local_index: TVerifiedSourceFileIndex, remote_index: TVerifiedSourceFileIndex
) -> Tuple[
    Dict[str, TVerifiedSourceFileEntry],
    Dict[str, TVerifiedSourceFileEntry],
    Dict[str, TVerifiedSourceFileEntry],
]:
    deleted: Dict[str, TVerifiedSourceFileEntry] = {}
    modified: Dict[str, TVerifiedSourceFileEntry] = {}
    new: Dict[str, TVerifiedSourceFileEntry] = {}

    for name, entry in remote_index["files"].items():
        if name not in local_index["files"]:
            new[name] = entry
        elif entry["sha3_256"] != local_index["files"][name]["sha3_256"]:
            modified[name] = entry

    for name, entry in local_index["files"].items():
        if name not in remote_index["files"]:
            deleted[name] = entry

    return new, modified, deleted


def find_conflict_files(
    local_index: TVerifiedSourceFileIndex,
    remote_new: Dict[str, TVerifiedSourceFileEntry],
    remote_modified: Dict[str, TVerifiedSourceFileEntry],
    remote_deleted: Dict[str, TVerifiedSourceFileEntry],
    dest_storage: FileStorage,
) -> Tuple[List[str], List[str]]:
    """Use files index from .sources to identify modified files via sha3 content hash"""

    conflict_modified: List[str] = []

    def is_file_modified(file: str, entry: TVerifiedSourceFileEntry) -> bool:
        with dest_storage.open_file(file, "rb") as f:
            file_blob = f.read()
        # file exists but was not changed
        return hashlib.sha3_256(file_blob).hexdigest() != entry["sha3_256"]

    for file, entry in remote_new.items():
        if dest_storage.has_file(file):
            # if incoming file is different from local
            if is_file_modified(file, entry):
                conflict_modified.append(file)
        else:
            # file is new local and remote
            pass

    for file, entry in remote_modified.items():
        if dest_storage.has_file(file):
            # if local file was changes and it is different from incoming
            if is_file_modified(file, entry) and is_file_modified(file, local_index["files"][file]):
                conflict_modified.append(file)
        else:
            # file was deleted but is modified on remote
            conflict_modified.append(file)

    conflict_deleted: List[str] = []
    for file, entry in remote_deleted.items():
        if dest_storage.has_file(file):
            if is_file_modified(file, entry):
                conflict_deleted.append(file)
        else:
            # file deleted locally and on remote -> ok
            pass

    return conflict_modified, conflict_deleted
