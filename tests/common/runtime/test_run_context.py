import os
from typing import Iterator
import pytest
import pickle

from dlt.common import logger
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.configuration.specs.run_configuration import RuntimeConfiguration
from dlt.common.runtime.init import initialize_runtime
from dlt.common.runtime.run_context import RunContext
from tests.utils import reload_run_context


@pytest.fixture(autouse=True)
def preserve_logger() -> Iterator[None]:
    old_logger = logger.LOGGER
    logger.LOGGER = None
    try:
        yield
    finally:
        logger.LOGGER = old_logger


def test_run_context() -> None:
    runtime_config = RuntimeConfiguration(name="dlt-test", data_dir="relative_dir")
    ctx = PluggableRunContext()
    run_context = ctx.context
    assert isinstance(run_context, RunContext)
    # regular settings before runtime_config applies
    assert run_context.name == "dlt"
    assert "relative_dir" not in run_context.data_dir
    assert run_context.global_dir == run_context.data_dir

    # check config providers
    assert len(run_context.initial_providers()) == 3

    # apply runtime config
    assert ctx.context.runtime_config is None
    ctx.init_runtime(runtime_config)
    # name and data_dir changed
    assert run_context.name == "dlt-test"
    assert "relative_dir" in run_context.data_dir

    # entities
    assert "data_entity" in run_context.get_data_entity("data_entity")
    # run entities are in run dir for default context
    assert "run_entity" not in run_context.get_run_entity("run_entity")
    assert run_context.get_run_entity("run_entity") == run_context.run_dir

    # check if can be pickled
    pickle.dumps(run_context)


def test_context_init_without_runtime() -> None:
    runtime_config = RuntimeConfiguration()
    ctx = PluggableRunContext()
    with Container().injectable_context(ctx):
        # logger is not initialized
        assert logger.LOGGER is None
        # runtime is also initialized but logger was not created
        assert ctx.context.runtime_config is not None
        # this will call init_runtime on injected context internally
        initialize_runtime(runtime_config)
        assert logger.LOGGER is not None
        assert ctx.context.runtime_config is runtime_config


def test_context_init_with_runtime() -> None:
    runtime_config = RuntimeConfiguration()
    ctx = PluggableRunContext()
    ctx.init_runtime(runtime_config)
    assert ctx.context.runtime_config is runtime_config
    # logger not initialized until placed in the container
    assert logger.LOGGER is None
    with Container().injectable_context(ctx):
        assert logger.LOGGER is not None


def test_context_switch_restores_logger() -> None:
    runtime_config = RuntimeConfiguration(name="dlt-tests")
    ctx = PluggableRunContext()
    ctx.init_runtime(runtime_config)
    with Container().injectable_context(ctx):
        assert logger.LOGGER.name == "dlt-tests"
        ctx = PluggableRunContext()
        ctx.init_runtime(RuntimeConfiguration(name="dlt-tests-2"))
        with Container().injectable_context(ctx):
            assert logger.LOGGER.name == "dlt-tests-2"
        assert logger.LOGGER.name == "dlt-tests"


def test_runtime_config_applied() -> None:
    import dlt

    # runtime configuration is loaded and applied immediately
    os.environ["RUNTIME__NAME"] = "runtime-cfg"
    os.environ["RUNTIME__DATA_DIR"] = "_storage"
    with reload_run_context():
        ctx = dlt.current.run()
        assert ctx.runtime_config.name == "runtime-cfg"
        assert ctx.name == "runtime-cfg"
        assert ctx.data_dir.endswith("_storage")
        assert os.path.isabs(ctx.data_dir)
