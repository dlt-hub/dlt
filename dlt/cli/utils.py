import ast
import inspect
import os
import tempfile
import time
import contextlib
from typing import Any, Callable, Tuple

from dlt.common import git
from dlt.common.reflection.utils import set_ast_parents
from dlt.common.storages import FileStorage
from dlt.common.typing import TFun
from dlt.common.runtime.telemetry import start_telemetry
from dlt.common.runtime.segment import track
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import RunConfiguration

from dlt.reflection.script_visitor import PipelineScriptVisitor

from dlt.cli.exceptions import CliCommandException


COMMAND_REPO_LOCATION = "https://github.com/dlt-hub/python-dlt-%s-template.git"
REQUIREMENTS_TXT = "requirements.txt"
PYPROJECT_TOML = "pyproject.toml"
GITHUB_WORKFLOWS_DIR = os.path.join(".github", "workflows")
LOCAL_COMMAND_REPO_FOLDER = "repos"
MODULE_INIT = "__init__.py"


def clone_command_repo(command: str, branch: str) -> FileStorage:
    template_dir = tempfile.mkdtemp()
    # TODO: handle ImportError (no git command available) gracefully
    with git.clone_repo(COMMAND_REPO_LOCATION % command, template_dir, branch=branch):
        return FileStorage(template_dir)


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


def track_command(command: str, track_before: bool, *args: str) -> Callable[[TFun], TFun]:
    """Adds telemetry to f: TFun and add optional f *args values to `properties` of telemetry event"""
    def decorator(f: TFun) -> TFun:
        sig: inspect.Signature = inspect.signature(f)
        def _wrap(*f_args: Any, **f_kwargs: Any) -> Any:
            # look for additional arguments
            bound_args = sig.bind(*f_args, **f_kwargs)
            props = {p:bound_args.arguments[p] for p in args if p in bound_args.arguments}
            start_ts = time.time()

            def _track(success: bool) -> None:
                with contextlib.suppress(Exception):
                    props["elapsed"] = time.time() - start_ts
                    props["success"] = success
                    # resolve runtime config and init telemetry
                    c = resolve_configuration(RunConfiguration())
                    start_telemetry(c)
                    track("command", command, props)

            # some commands should be tracked before execution
            if track_before:
                _track(True)
                return f(*f_args, **f_kwargs)
            # some commands we track after, where we can pass the success
            try:
                rv = f(*f_args, **f_kwargs)
                _track(rv == 0)
                return rv
            except Exception:
                _track(False)
                raise

        return _wrap  # type: ignore
    return decorator


def get_telemetry_status() -> bool:
    c = resolve_configuration(RunConfiguration())
    return c.dlthub_telemetry
