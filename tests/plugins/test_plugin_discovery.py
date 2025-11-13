from subprocess import CalledProcessError
import pytest
import os
import sys
import tempfile
import shutil
import importlib

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.destination import DestinationReference
from dlt.common.runners import Venv
from dlt.common.configuration import plugins
from dlt.common.configuration.plugins import PluginContext
from dlt.common.runtime import run_context

from dlt.sources import SourceReference
from tests.utils import TEST_STORAGE_ROOT
from pytest_console_scripts import ScriptRunner


@pytest.fixture(scope="module", autouse=True)
def plugin_install():
    # install plugin into temp dir
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

    # remove current plugin manager and run context
    # NOTE: new run context reloads plugin manager
    container = Container()
    if PluggableRunContext in container:
        del container[PluggableRunContext]
    if plugins.PluginContext in container:
        del container[plugins.PluginContext]

    # reload metadata module
    importlib.reload(importlib.metadata)

    yield

    # remove distribution search, temp package and plugin manager
    sys.path.remove(temp_dir)
    shutil.rmtree(temp_dir)
    importlib.reload(importlib.metadata)
    del container[plugins.PluginContext]


def test_example_plugin() -> None:
    context = run_context.active()
    assert context.name == "dlt-test"
    # run_dir is the module file path and we have profile == dev as runtime kwargs
    assert context.uri.endswith("/dlt_example_plugin?profile=dev")
    assert context.data_dir == os.path.abspath(TEST_STORAGE_ROOT)
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

    # raise
    result = script_runner.run(["dlt", "example", "--name", "John", "--result", "known_error"])
    assert result.returncode == -33
    assert "MODIFIED_DOCS_URL" in result.stdout

    result = script_runner.run(["dlt", "example", "--name", "John", "--result", "unknown_error"])
    assert result.returncode == -1
    assert "DEFAULT_DOCS_URL" in result.stdout
    assert "No one knows what is going on" in result.stderr
    assert "Traceback" not in result.stderr  # stack trace is not there

    # raise with trace
    result = script_runner.run(
        ["dlt", "--debug", "example", "--name", "John", "--result", "unknown_error"]
    )
    assert "No one knows what is going on" in result.stderr
    assert "Traceback" in result.stderr  # stacktrace is there

    # overwritten pipeline command
    result = script_runner.run(["dlt", "init"])
    assert result.returncode == -55
    assert "Plugin overwrote init command" in result.stdout
    assert "INIT_DOCS_URL" in result.stdout
