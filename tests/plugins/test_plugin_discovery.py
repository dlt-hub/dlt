from subprocess import CalledProcessError
import pytest
import os
import sys
import tempfile
import shutil
import importlib

from dlt.common.configuration.container import Container
from dlt.common.runners import Venv
from dlt.common.configuration import plugins
from dlt.common.runtime import run_context
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
                "pip", "install", "tests/plugins/dlt_example_plugin", "--target", temp_dir
            )
        )
    except CalledProcessError as c_err:
        print(c_err.stdout)
        print(c_err.stderr)
        raise
    sys.path.insert(0, temp_dir)

    # remove current plugin manager
    container = Container()
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
    context = run_context.current()
    assert context.name == "dlt-test"
    assert context.data_dir == os.path.abspath(TEST_STORAGE_ROOT)


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
