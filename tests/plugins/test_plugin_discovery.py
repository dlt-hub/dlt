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
