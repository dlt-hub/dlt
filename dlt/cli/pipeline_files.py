import fnmatch
import hashlib
import os
import yaml
import posixpath
from pathlib import Path
from typing import Dict, NamedTuple, Sequence, Tuple, TypedDict, List
from dlt.cli.exceptions import PipelineRepoError

from dlt.common import git
from dlt.common.configuration.paths import make_dlt_project_path
from dlt.common.storages import FileStorage

from dlt.common.reflection.utils import get_module_docstring

from dlt.cli import utils


PIPELINES_INIT_INFO_ENGINE_VERSION = 1
PIPELINES_INIT_INFO_FILE = ".pipelines"
IGNORE_FILES = ["*.py[cod]", "*$py.class", "__pycache__", "py.typed", "requirements.txt"]
IGNORE_PIPELINES = [".*", "_*"]


class PipelineFiles(NamedTuple):
    is_template: bool
    storage: FileStorage
    pipeline_script: str
    dest_pipeline_script: str
    files: List[str]
    requirements: List[str]
    doc: str


class TPipelineFileEntry(TypedDict):
    commit_sha: str
    git_sha: str
    sha3_256: str


class TPipelineFileIndex(TypedDict):
    is_dirty: bool
    last_commit_sha: str
    last_commit_timestamp: str
    files: Dict[str, TPipelineFileEntry]


class TPipelinesFileIndex(TypedDict):
    engine_version: int
    pipelines: Dict[str, TPipelineFileIndex]


def _save_dot_pipelines(index: TPipelinesFileIndex) -> None:
    with open(make_dlt_project_path(PIPELINES_INIT_INFO_FILE), "w", encoding="utf-8") as f:
        yaml.dump(index, f, allow_unicode=True, default_flow_style=False, sort_keys=False)


def _load_dot_pipelines() -> TPipelinesFileIndex:
    try:
        with open(make_dlt_project_path(PIPELINES_INIT_INFO_FILE), "r", encoding="utf-8") as f:
            index: TPipelinesFileIndex = yaml.safe_load(f)
            if not index:
                raise FileNotFoundError(PIPELINES_INIT_INFO_FILE)
            return index
    except FileNotFoundError:
        return {
            "engine_version": PIPELINES_INIT_INFO_ENGINE_VERSION,
            "pipelines": {}
        }


def _merge_remote_index(
    local_index: TPipelineFileIndex,
    remote_index: TPipelineFileIndex,
    remote_modified: Dict[str, TPipelineFileEntry],
    remote_deleted: Dict[str, TPipelineFileEntry]
) -> TPipelineFileIndex:
    # update all modified files
    local_index["files"].update(remote_modified)
    # delete all deleted
    for deleted in remote_deleted:
        del local_index["files"][deleted]
    # update global info
    local_index["is_dirty"] = remote_index["is_dirty"]
    local_index["last_commit_sha"] = remote_index["last_commit_sha"]
    local_index["last_commit_timestamp"] = remote_index["last_commit_timestamp"]

    return local_index


def load_pipeline_local_index(pipeline_name: str) -> TPipelineFileIndex:
    return _load_dot_pipelines()["pipelines"].get(pipeline_name, {
        "is_dirty": False,
        "last_commit_sha": None,
        "last_commit_timestamp": None,
        "files": {}
        }
    )


def save_pipeline_local_index(
    pipeline_name: str,
    remote_index: TPipelineFileIndex,
    remote_modified: Dict[str, TPipelineFileEntry],
    remote_deleted: Dict[str, TPipelineFileEntry]
) -> None:

    all_pipelines = _load_dot_pipelines()
    local_index = all_pipelines["pipelines"].setdefault(pipeline_name, remote_index)
    _merge_remote_index(local_index, remote_index, remote_modified, remote_deleted)
    _save_dot_pipelines(all_pipelines)


def get_remote_pipeline_index(repo_path: str, files: Sequence[str]) -> TPipelineFileIndex:

    with git.get_repo(repo_path) as repo:
        tree = repo.tree()
        commit_sha = repo.head.commit.hexsha
        files_sha: Dict[str, TPipelineFileEntry] = {}
        for file in files:
            posix_file = os.path.join(repo_path, file)
            posix_file = os.path.relpath(posix_file, repo.working_dir)
            posix_file = posixpath.join(*Path(posix_file).parts)
            try:
                blob_sha3 = tree.join(posix_file).hexsha
            except KeyError:
                # if directory is dirty and we do not have git sha
                blob_sha3 = None

            with open(os.path.join(repo_path, file), "br") as f:
                file_blob = f.read()
            files_sha[file] = {
                "commit_sha": commit_sha,
                "git_sha": blob_sha3,
                "sha3_256":  hashlib.sha3_256(file_blob).hexdigest()
            }

        return {
            "is_dirty": git.is_dirty(repo),
            "last_commit_sha": commit_sha,
            "last_commit_timestamp": repo.head.commit.committed_datetime.isoformat(),
            "files": files_sha
        }


def get_pipeline_names(pipelines_storage: FileStorage) -> List[str]:
    candidates: List[str] = []
    for name in [n for n in pipelines_storage.list_folder_dirs(".", to_root=False) if not any(fnmatch.fnmatch(n, ignore) for ignore in IGNORE_PIPELINES)]:
        # must contain at least one valid python script
        if any(f.endswith(".py") for f in pipelines_storage.list_folder_files(name, to_root=False)):
            candidates.append(name)
    return candidates


def get_pipeline_files(pipelines_storage: FileStorage, pipeline_name: str) -> PipelineFiles:
    if not pipelines_storage.has_folder(pipeline_name):
        raise PipelineRepoError(f"Pipeline {pipeline_name} could not be found in the repository", pipeline_name)
    # find example script
    example_script = f"{pipeline_name}_pipeline.py"
    if not pipelines_storage.has_file(example_script):
        raise PipelineRepoError(f"Pipeline example script {example_script} could not be found in the repository", pipeline_name)
    # get all files recursively
    files: List[str] = []
    for root, subdirs, _files in os.walk(pipelines_storage.make_full_path(pipeline_name)):
        # filter unwanted files
        for subdir in list(subdirs):
            if any(fnmatch.fnmatch(subdir, ignore) for ignore in IGNORE_FILES):
                subdirs.remove(subdir)
        rel_root = pipelines_storage.to_relative_path(root)
        files.extend([os.path.join(rel_root, file) for file in _files if all(not fnmatch.fnmatch(file, ignore) for ignore in IGNORE_FILES)])
    # read the docs
    init_py =  os.path.join(pipeline_name, utils.MODULE_INIT)
    docstring: str = ""
    if pipelines_storage.has_file(init_py):
        docstring = get_module_docstring(pipelines_storage.load(init_py))
        if docstring:
            docstring = docstring.splitlines()[0]
    # read requirements
    requirements_path = os.path.join(pipeline_name, utils.REQUIREMENTS_TXT)
    if pipelines_storage.has_file(requirements_path):
        requirements = pipelines_storage.load(requirements_path).splitlines()
    else:
        requirements = []
    # find requirements
    return PipelineFiles(False, pipelines_storage, example_script, example_script, files, requirements, docstring)


def gen_index_diff(
    local_index: TPipelineFileIndex,
    remote_index: TPipelineFileIndex
) -> Tuple[Dict[str, TPipelineFileEntry], Dict[str, TPipelineFileEntry], Dict[str, TPipelineFileEntry]]:

    deleted: Dict[str, TPipelineFileEntry] = {}
    modified: Dict[str, TPipelineFileEntry] = {}
    new: Dict[str, TPipelineFileEntry] = {}

    for name, entry in remote_index["files"].items():
        if name not in local_index["files"]:
            new[name] = entry
        elif entry["sha3_256"] != local_index["files"][name]["sha3_256"]:
            modified[name] = entry

    for name, entry in local_index["files"].items():
        if name not in remote_index["files"]:
            deleted[name] = entry

    # print("NEW")
    # print(new)
    # print("MOD")
    # print(modified)
    # print("DEL")
    # print(deleted)
    return new, modified, deleted


def find_conflict_files(
    local_index: TPipelineFileIndex,
    remote_new: Dict[str, TPipelineFileEntry],
    remote_modified: Dict[str, TPipelineFileEntry],
    remote_deleted: Dict[str, TPipelineFileEntry],
    dest_storage: FileStorage
) -> Tuple[List[str], List[str]]:
    """Use files index from .pipelines to identify modified files via sha3 content hash"""

    conflict_modified: List[str] = []

    def is_file_modified(file: str, entry: TPipelineFileEntry) -> bool:
        with dest_storage.open_file(file, "br") as f:
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
