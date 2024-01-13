from typing import List

import dlt
import pytest
import os

from dlt.common.plugins import CallbackPlugin, PluginsContext
from dlt.common.pipeline import SupportsPipeline
from dlt.common.plugins.exceptions import UnknownPluginPathException
from dlt.common.configuration.specs.base_configuration import BaseConfiguration
from dlt.common.configuration.specs.base_configuration import configspec
from tests.common.configuration.utils import (
    environment,
)
from dlt.common.configuration.exceptions import ConfigFieldMissingException


class StepCounterPlugin(CallbackPlugin[BaseConfiguration]):
    def __init__(self) -> None:
        super().__init__()
        self.start_steps: List[str] = []
        self.end_steps: List[str] = []
        self.on_start_called: int = 0
        self.on_end_called: int = 0

    def on_step_start(self, step: str, pipeline: SupportsPipeline) -> None:
        self.start_steps.append(step)

    def on_step_end(self, step: str, pipeline: SupportsPipeline) -> None:
        self.end_steps.append(step)

    def on_start(self, pipeline: SupportsPipeline) -> None:
        self.on_start_called += 1

    def on_end(self, pipeline: SupportsPipeline) -> None:
        self.on_end_called += 1


def test_simple_plugin_steps() -> None:
    """very simple test to see if plugins work"""
    pipeline = dlt.pipeline(
        "my_pipeline_2", plugins=[StepCounterPlugin], destination="dummy", full_refresh=True
    )
    pipeline.run([{"a": 1, "b": 2}], table_name="my_table")

    plug = pipeline.get_plugin("stepcounterplugin")

    assert plug.start_steps == ["run", "extract", "normalize", "load"]
    assert plug.end_steps == ["extract", "normalize", "load", "run"]
    assert plug.on_start_called == 1
    assert plug.on_end_called == 1
    assert pipeline._last_plugin_ctx is not None
    assert pipeline._plugin_ctx is None


def test_plugin_resolution() -> None:
    ctx = PluginsContext()

    # class gets instantiated
    assert isinstance(ctx._resolve_plugin(StepCounterPlugin), StepCounterPlugin)

    # string gets imported and instantiated
    assert isinstance(
        ctx._resolve_plugin("tests.common.plugins.test_plugin_basics.StepCounterPlugin"),
        StepCounterPlugin,
    )

    # proper error messages if plugin string cannot be resolved
    with pytest.raises(UnknownPluginPathException):
        ctx._resolve_plugin("path.does.not.exist.StepCounterPlugin")

    # same but incorrect attribute
    with pytest.raises(UnknownPluginPathException):
        ctx._resolve_plugin("tests.common.plugins.test_plugin_basics.UnknwonAttr")

    # resolves but class is not plugin type
    with pytest.raises(TypeError):
        ctx._resolve_plugin("dlt.common.plugins.PluginsContext")

    # other incorrect types
    with pytest.raises(TypeError):
        ctx._resolve_plugin(1)  # type: ignore

    with pytest.raises(TypeError):
        ctx._resolve_plugin(ctx)  # type: ignore

    # instances are not allowed
    with pytest.raises(TypeError):
        ctx._resolve_plugin(StepCounterPlugin())


@configspec
class PluginConfig(BaseConfiguration):
    some_string: str
    some_integer: int


class TestConfigPlugin(CallbackPlugin[PluginConfig]):
    SPEC = PluginConfig


class TestConfigPluginWithName(CallbackPlugin[PluginConfig]):
    SPEC = PluginConfig
    NAME = "some_name"


def test_plugin_config_resolution_basic_vars(environment) -> None:
    os.environ["PLUGIN__SOME_STRING"] = "some_string"
    os.environ["PLUGIN__SOME_INTEGER"] = "123"
    plug = TestConfigPlugin()
    assert plug.config.some_string == "some_string"
    assert plug.config.some_integer == 123


def test_plugin_config_resolution_name_section(environment) -> None:
    os.environ["PLUGIN__SOME_STRING"] = "wrong_string"
    os.environ["PLUGIN__SOME_INTEGER"] = "666"

    os.environ["PLUGIN__SOME_NAME__SOME_STRING"] = "some_string"
    os.environ["PLUGIN__SOME_NAME__SOME_INTEGER"] = "111"

    os.environ["PLUGIN__TESTCONFIGPLUGIN__SOME_STRING"] = "test_string"
    os.environ["PLUGIN__TESTCONFIGPLUGIN__SOME_INTEGER"] = "333"

    plug1 = TestConfigPluginWithName()
    assert plug1.config.some_string == "some_string"
    assert plug1.config.some_integer == 111
    assert plug1.NAME == "some_name"

    plug2 = TestConfigPlugin()
    assert plug2.config.some_string == "test_string"
    assert plug2.config.some_integer == 333
    assert plug2.NAME == "testconfigplugin"


def test_plugin_config_resolution_no_vars(environment) -> None:
    with pytest.raises(ConfigFieldMissingException):
        TestConfigPlugin()
