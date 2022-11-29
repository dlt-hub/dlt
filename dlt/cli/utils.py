import ast
import os
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
    sys.path.append(template_dir)
    try:
        return import_module(template_module_name)
    finally:
        sys.path.remove(template_dir)


def parse_init_script(command: str, script_source: str, init_script_name: str) -> PipelineScriptVisitor:
    # parse the script first
    tree = ast.parse(source=script_source)
    set_ast_parents(tree)
    visitor = PipelineScriptVisitor(script_source)
    visitor.visit(tree)
    if len(visitor.mod_aliases) == 0:
        raise CliCommandException(command, f"The pipeline script {init_script_name} does not import dlt and does not seem to run any pipelines")
    if n.RUN not in visitor.known_calls:
        raise CliCommandException(command, f"The pipeline script {init_script_name} does not seem to run the pipeline.")

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
