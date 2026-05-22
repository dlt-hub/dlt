from subprocess import CalledProcessError
import pytest
import os
import sys
import tempfile
import shutil
import importlib
from typing import Any, Dict, List, Optional, Set, Type

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.destination import DestinationReference
from dlt.common.runners import Venv
from dlt.common.configuration import plugins
from dlt.common.configuration.plugins import PluginContext, SupportsCliCommand
from dlt.common.runtime import run_context

from dlt.sources import SourceReference
from dlt._workspace.cli._dlt import _create_parser
from tests.utils import get_test_storage_root
from pytest_console_scripts import ScriptRunner

pytestmark = pytest.mark.serial


@pytest.fixture(scope="module", autouse=True)
def plugin_install():
    # clean non-idempotent build artifacts
    shutil.rmtree("tests/plugins/dlt_example_plugin/build", ignore_errors=True)
    shutil.rmtree(
        "tests/plugins/dlt_example_plugin/dlt_example_plugin.egg-info", ignore_errors=True
    )

    temp_dir = tempfile.mkdtemp()
    venv = Venv.restore_current()
    try:
        print(
            venv.run_module(
                "pip",
                "install",
                "tests/plugins/dlt_example_plugin",
                "--target",
                temp_dir,
            )
        )
    except CalledProcessError as c_err:
        print(c_err.stdout)
        print(c_err.stderr)
        raise
    sys.path.insert(0, temp_dir)

    # a new run context reloads the plugin manager
    container = Container()
    if PluggableRunContext in container:
        del container[PluggableRunContext]
    if plugins.PluginContext in container:
        del container[plugins.PluginContext]

    importlib.reload(importlib.metadata)

    yield

    sys.path.remove(temp_dir)
    shutil.rmtree(temp_dir)
    importlib.reload(importlib.metadata)
    del container[plugins.PluginContext]


def test_example_plugin() -> None:
    context = run_context.active()
    assert context.name == "dlt-test"
    # `profile=dev` comes from runtime kwargs, `run_dir` is the plugin module path
    assert context.uri.endswith("/dlt_example_plugin?profile=dev")
    assert context.data_dir == os.path.abspath(get_test_storage_root())
    # top level module info should be present
    assert context.module.__name__ == "dlt_example_plugin"
    # plugin manager should contain the plugin module
    plugin_context = Container()[PluginContext]
    assert plugin_context.plugin_modules == [context.module.__name__, "dlt"]
    # reference prefixes we probe when resolving
    assert run_context.get_plugin_modules() == ["dlt_example_plugin", "dlt"]
    assert context.local_dir.startswith(context.data_dir)
    assert context.local_dir.endswith("tmp")


def test_run_context_passthrough() -> None:
    context = run_context.active()
    assert context.name == "dlt-test"

    try:
        container = Container()
        container[PluggableRunContext].reload(context.run_dir, dict(passthrough=True))

        context = run_context.active()
        assert context.name == "dlt"

    finally:
        container[PluggableRunContext].reload(context.run_dir, dict(passthrough=False))
        context = run_context.active()
        assert context.name == "dlt-test"


def test_import_references() -> None:
    # unknown
    with pytest.raises(KeyError):
        SourceReference.find("unknown")
    # find also imports
    source_ref = SourceReference.find("github")
    assert source_ref.ref.name == "github"
    assert source_ref.ref.section == "github"
    assert source_ref.ref.ref == "dlt_example_plugin.sources.github.github"

    # create default instance
    assert SourceReference.from_reference("github") is not None

    with pytest.raises(KeyError):
        DestinationReference.find("unknown")

    # imports destinations
    dest_t = DestinationReference.find("hive")
    dest_f = DestinationReference.from_reference("hive")
    assert type(dest_f) is dest_t

    assert dest_f.destination_name == "hive"
    assert dest_f.destination_type == "dlt_example_plugin.destinations.hive"
    dest_f = DestinationReference.from_reference("push_destination")
    assert dest_f.destination_name == "pushdb"
    assert (
        dest_f.destination_type
        == "dlt_example_plugin.destinations.pushdb.PushDestinationDestination"
    )


def test_plugin_execution_context() -> None:
    from dlt.common.runtime.exec_info import get_execution_context

    context = get_execution_context()
    assert context["run_context"] == "dlt-test"


def test_cli_hook(script_runner: ScriptRunner) -> None:
    # new command
    result = script_runner.run(["dlt", "example", "--name", "John"])
    assert result.returncode == 0
    assert "Example command executed with name: John" in result.stdout

    # known error: command-defined exit code and docs URL
    result = script_runner.run(["dlt", "example", "--name", "John", "--result", "known_error"])
    assert result.returncode == -33
    assert "MODIFIED_DOCS_URL" in result.stdout

    # unknown error: stack trace is suppressed without `--debug`
    result = script_runner.run(["dlt", "example", "--name", "John", "--result", "unknown_error"])
    assert result.returncode == -1
    assert "DEFAULT_DOCS_URL" in result.stdout
    assert "No one knows what is going on" in result.stderr
    assert "Traceback" not in result.stderr

    # `--debug` keeps the stack trace
    result = script_runner.run(
        ["dlt", "--debug", "example", "--name", "John", "--result", "unknown_error"]
    )
    assert "No one knows what is going on" in result.stderr
    assert "Traceback" in result.stderr

    # plugin overwrites the built-in `init`
    result = script_runner.run(["dlt", "init"])
    assert result.returncode == -55
    assert "Plugin overwrote init command" in result.stdout
    assert "INIT_DOCS_URL" in result.stdout

    # legacy hookimpl (no host param) keeps registering on the dlt host via pluggy default
    result = script_runner.run(["dlt", "legacy"])
    assert result.returncode == 0
    assert "Legacy command executed" in result.stdout


def test_cli_hook_host_filtering() -> None:
    """Built-in hookimpls return None for unknown hosts; legacy impls register everywhere."""
    m = plugins.manager()

    def top_level_names(results: List[Optional[Type[SupportsCliCommand]]]) -> Set[str]:
        return {
            c().command for c in results if c is not None and getattr(c, "parent", None) is None
        }

    # dlt-only built-ins + example plugin commands + legacy. No `ai` (dlthub-only) and no t_*.
    dlt_results: List[Optional[Type[SupportsCliCommand]]] = m.hook.plug_cli(host="dlt")
    assert top_level_names(dlt_results) == {
        "ai",  # ai has moved... stub
        "init",
        "pipeline",
        "schema",
        "telemetry",
        "deploy",
        "dashboard",
        "example",
        "legacy",
    }

    # `pipeline`, `local`, `profile`, `info` are gated on `is_workspace_active()` and absent here
    # — they are asserted in tests/workspace/cli/dlthub/test_local_workspace_command.py.
    # `schema`, `telemetry`, `workspace`, `deploy`, `dashboard` are not exposed on dlthub.
    dlthub_results: List[Optional[Type[SupportsCliCommand]]] = m.hook.plug_cli(host="dlthub")
    assert top_level_names(dlthub_results) == {
        "ai",
        "init",
        "t_info",
        "t_workspace",
        "example",
        "legacy",
    }

    # legacy impl has no `host` param so pluggy keeps calling it on any host
    other_results: List[Optional[Type[SupportsCliCommand]]] = m.hook.plug_cli(host="other")
    assert top_level_names(other_results) == {"legacy"}


def _build_and_run(monkeypatch: pytest.MonkeyPatch, host: str, argv: List[str]) -> None:
    """Builds the parser for `host`, parses `argv`, and executes the resolved top-level command."""
    monkeypatch.setattr(sys, "argv", [host, *argv])
    parser, pre_parser, installed = _create_parser(host)
    ns, remaining = pre_parser.parse_known_args(argv)
    parsed = parser.parse_args(remaining, namespace=ns)
    installed[parsed.command].execute(parsed)


def test_compose_extend_runs_all_executes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _build_and_run(monkeypatch, "dlthub", ["t_info", "--detailed"])
    out = capsys.readouterr().out
    assert "t_info A: detailed=True" in out
    assert "t_info B: detailed=True" in out


def test_compose_additive_adds_subcommand(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _build_and_run(monkeypatch, "dlthub", ["t_workspace", "switch", "--target", "prod"])
    assert "t_workspace switch: target=prod" in capsys.readouterr().out
