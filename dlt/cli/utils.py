import ast
import os
from pathlib import Path
import sys
import tempfile
from importlib import import_module
from types import ModuleType

from dlt.common.git import clone_repo
from dlt.common.reflection.utils import set_ast_parents
from dlt.common.storages import FileStorage

from dlt.reflection.script_visitor import PipelineScriptVisitor
import dlt.reflection.names as n

from dlt.cli.exceptions import CliCommandException


COMMAND_REPO_LOCATION = "https://github.com/dlt-hub/python-dlt-%s-template.git"
REQUIREMENTS_TXT = "requirements.txt"
PYPROJECT_TOML = "pyproject.toml"
GITHUB_WORKFLOWS_DIR = os.path.join(".github", "workflows")


def clone_command_repo(command: str, branch: str) -> FileStorage:
    template_dir = tempfile.mkdtemp()
    # TODO: handle ImportError (no git command available) gracefully
    clone_repo(COMMAND_REPO_LOCATION % command, template_dir, branch=branch)

    return FileStorage(template_dir)


def load_command_module(template_dir: str) -> ModuleType:
    # import the settings from the clone
    template_dir, template_module_name = os.path.split(template_dir.rstrip("/"))
    module_path, package = os.path.split(template_dir)
    module, _ = os.path.splitext(template_module_name)
    module = ".".join(Path(module).parts)

    sys_path: str = None
    if module_path not in sys.path:
        sys_path = module_path
        # path must be first so we always load our module of
        sys.path.insert(0, sys_path)
    try:
        return import_module(f"{package}.{module}")
    finally:
        sys.path.remove(sys_path)


def parse_init_script(command: str, script_source: str, init_script_name: str) -> PipelineScriptVisitor:
    # parse the script first
    tree = ast.parse(source=script_source)
    set_ast_parents(tree)
    visitor = PipelineScriptVisitor(script_source)
    visitor.visit_passes(tree)
    if len(visitor.mod_aliases) == 0:
        raise CliCommandException(command, f"The pipeline script {init_script_name} does not import dlt and does not seem to run any pipelines")

    return visitor


def ensure_git_command(command: str) -> None:
    try:
        import git
    except ImportError as imp_ex:
        if "Bad git executable" not in str(imp_ex):
            raise
        raise CliCommandException(
            command,
            "'git' command is not available. Install and setup git with the following the guide %s" % "https://docs.github.com/en/get-started/quickstart/set-up-git",
            imp_ex
        ) from imp_ex
