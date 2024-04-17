import ast
import os
from typing import Callable

from dlt.common.reflection.utils import set_ast_parents
from dlt.common.typing import TFun
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import RunConfiguration
from dlt.common.runtime.telemetry import with_telemetry

from dlt.reflection.script_visitor import PipelineScriptVisitor

from dlt.cli.exceptions import CliCommandException


REQUIREMENTS_TXT = "requirements.txt"
PYPROJECT_TOML = "pyproject.toml"
GITHUB_WORKFLOWS_DIR = os.path.join(".github", "workflows")
AIRFLOW_DAGS_FOLDER = os.path.join("dags")
AIRFLOW_BUILD_FOLDER = os.path.join("build")
LOCAL_COMMAND_REPO_FOLDER = "repos"
MODULE_INIT = "__init__.py"


def parse_init_script(
    command: str, script_source: str, init_script_name: str
) -> PipelineScriptVisitor:
    # parse the script first
    tree = ast.parse(source=script_source)
    set_ast_parents(tree)
    visitor = PipelineScriptVisitor(script_source)
    visitor.visit_passes(tree)
    if len(visitor.mod_aliases) == 0:
        raise CliCommandException(
            command,
            f"The pipeline script {init_script_name} does not import dlt and does not seem to run"
            " any pipelines",
        )

    return visitor


def ensure_git_command(command: str) -> None:
    try:
        import git
    except ImportError as imp_ex:
        if "Bad git executable" not in str(imp_ex):
            raise
        raise CliCommandException(
            command,
            "'git' command is not available. Install and setup git with the following the guide %s"
            % "https://docs.github.com/en/get-started/quickstart/set-up-git",
            imp_ex,
        ) from imp_ex


def track_command(command: str, track_before: bool, *args: str) -> Callable[[TFun], TFun]:
    return with_telemetry("command", command, track_before, *args)


def get_telemetry_status() -> bool:
    c = resolve_configuration(RunConfiguration())
    return c.dlthub_telemetry
