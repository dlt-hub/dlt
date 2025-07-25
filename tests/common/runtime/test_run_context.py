import os
from typing import Iterator
import pytest
import pickle

from dlt.common import logger
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import RuntimeConfiguration, PluggableRunContext
from dlt.common.runtime.init import _INITIALIZED, apply_runtime_config, restore_run_context
from dlt.common.runtime.run_context import (
    DOT_DLT,
    RunContext,
    get_plugin_modules,
    is_folder_writable,
)
from dlt.common.storages.configuration import _make_file_url
from dlt.common.utils import set_working_dir

import tests
from tests.utils import MockableRunContext, TEST_STORAGE_ROOT


@pytest.fixture(autouse=True)
def preserve_logger() -> Iterator[None]:
    old_logger = logger.LOGGER
    logger.LOGGER = None
    try:
        yield
    finally:
        logger.LOGGER = old_logger


@pytest.fixture(autouse=True)
def preserve_run_context() -> Iterator[None]:
    container = Container()
    old_ctx = container[PluggableRunContext]
    try:
        yield
    finally:
        container[PluggableRunContext] = old_ctx


def test_run_context() -> None:
    ctx = PluggableRunContext()
    run_context = ctx.context
    assert isinstance(run_context, RunContext)
    # regular settings before runtime_config applies
    assert run_context.name == "dlt"
    assert run_context.global_dir == run_context.data_dir
    assert run_context.run_dir == run_context.local_dir
    assert run_context.uri == _make_file_url(None, run_context.run_dir, None)
    assert run_context.uri.startswith("file://")

    # check config providers
    assert len(run_context.initial_providers()) == 3

    # apply runtime config
    assert ctx.runtime_config is None
    ctx.add_extras()
    assert ctx.runtime_config is not None

    runtime_config = RuntimeConfiguration()
    ctx.initialize_runtime(runtime_config)
    assert ctx.runtime_config is runtime_config

    # entities
    assert "data_entity" in run_context.get_data_entity("data_entity")
    # run entities are in run dir for default context
    assert "run_entity" not in run_context.get_run_entity("run_entity")
    assert run_context.get_run_entity("run_entity") == run_context.run_dir

    # check if can be pickled
    pickle.dumps(run_context)

    # check plugin modules
    # NOTE: first `dlt` - is the root module of current context, second is always present
    assert get_plugin_modules() == ["dlt", "dlt"]


def test_context_without_module() -> None:
    with set_working_dir(TEST_STORAGE_ROOT):
        ctx = PluggableRunContext()
        with Container().injectable_context(ctx):
            assert ctx.context.module is None
            assert get_plugin_modules() == ["", "dlt"]


def test_context_init_without_runtime() -> None:
    runtime_config = RuntimeConfiguration()
    ctx = PluggableRunContext()
    with Container().injectable_context(ctx):
        # logger is immediately initialized
        assert logger.LOGGER is not None
        # runtime is also initialized but logger was not created
        assert ctx.runtime_config is not None
        # this will call init_runtime on injected context internally
        apply_runtime_config(runtime_config)
        assert logger.LOGGER is not None
        assert ctx.runtime_config is runtime_config


def test_context_init_with_runtime() -> None:
    runtime_config = RuntimeConfiguration()
    ctx = PluggableRunContext(runtime_config=runtime_config)
    assert ctx.runtime_config is runtime_config
    # logger not initialized until placed in the container
    assert logger.LOGGER is None
    with Container().injectable_context(ctx):
        assert ctx.runtime_config is runtime_config
        assert logger.LOGGER is not None


def test_run_context_handover() -> None:
    runtime_config = RuntimeConfiguration()
    ctx = PluggableRunContext()
    mock = MockableRunContext.from_context(ctx.context)
    mock._name = "handover-dlt"
    # also adds to context, should initialize runtime
    global _INITIALIZED
    try:
        telemetry_init = _INITIALIZED
        # do not initialize telemetry here
        _INITIALIZED = True
        restore_run_context(mock, runtime_config)
    finally:
        _INITIALIZED = telemetry_init

    # logger initialized and named
    assert logger.LOGGER.name == "handover-dlt"

    # get regular context
    import dlt

    run_ctx = dlt.current.run_context()
    assert run_ctx is mock
    ctx = Container()[PluggableRunContext]
    assert ctx.runtime_config is runtime_config


def test_context_switch_restores_logger() -> None:
    ctx = PluggableRunContext()
    mock = MockableRunContext.from_context(ctx.context)
    mock._name = "dlt-tests"
    ctx.context = mock
    with Container().injectable_context(ctx):
        assert logger.LOGGER.name == "dlt-tests"
        ctx = PluggableRunContext()
        mock = MockableRunContext.from_context(ctx.context)
        mock._name = "dlt-tests-2"
        ctx.context = mock
        with Container().injectable_context(ctx):
            assert logger.LOGGER.name == "dlt-tests-2"
        assert logger.LOGGER.name == "dlt-tests"


def test_run_dir_module_import() -> None:
    with pytest.raises(ImportError, match="filesystem root"):
        RunContext.import_run_dir_module(os.path.sep)
    with pytest.raises(ImportError):
        RunContext.import_run_dir_module(os.path.join("tests", "no_such_module"))
    assert RunContext.import_run_dir_module("tests") is tests


def test_tmp_folder_writable() -> None:
    import tempfile

    assert is_folder_writable(tempfile.gettempdir()) is True


def test_context_with_xdg_dir(mocker) -> None:
    import tempfile

    temp_data_home = tempfile.mkdtemp()
    mock_expanded_user_dir = tempfile.mkdtemp()

    # mock os.path.expanduser to return a different temp folder
    with mocker.patch("os.path.expanduser", return_value=mock_expanded_user_dir):
        os.environ["XDG_DATA_HOME"] = temp_data_home

        ctx = PluggableRunContext()
        run_context = ctx.context
        assert run_context.global_dir == os.path.join(temp_data_home, "dlt")

        # now create .dlt in mocked home to activate callback
        dlt_home = os.path.join(mock_expanded_user_dir, DOT_DLT)
        os.mkdir(dlt_home)

        ctx = PluggableRunContext()
        run_context = ctx.context
        assert run_context.global_dir == dlt_home
