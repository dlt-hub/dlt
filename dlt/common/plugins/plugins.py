from typing import List, Any, Iterable
from dlt.common.typing import TFun
from dlt.common.typing import TDataItem
from dlt.common.pipeline import SupportsPipeline
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.schema.typing import TSchemaContract
from dlt.common.configuration.specs import configspec
from functools import wraps
from .reference import SupportsCallbackPlugin, Plugin, TSinglePluginArg, TPluginArg, CallbackPlugin
from dlt.common.configuration.container import Container
from dlt.common.schema.exceptions import DataValidationError
from importlib import import_module
from .exceptions import UnknownPluginPathException
from dlt.common.configuration.specs.base_configuration import BaseConfiguration


@configspec
class PluginsContext(ContainerInjectableContext, SupportsCallbackPlugin):
    def __init__(self) -> None:
        self._plugins: List[Plugin[BaseConfiguration]] = []
        self._callback_plugins: List[CallbackPlugin[BaseConfiguration]] = []

    def _resolve_plugin(self, plugin: TSinglePluginArg) -> Plugin[BaseConfiguration]:
        resolved_plugin: Plugin[BaseConfiguration] = None
        if isinstance(plugin, str):
            module_path, attr_name = plugin.rsplit(".", 1)
            try:
                module = import_module(module_path)
            except ModuleNotFoundError as e:
                raise UnknownPluginPathException(plugin) from e
            try:
                plugin = getattr(module, attr_name)
            except AttributeError as e:
                raise UnknownPluginPathException(str(plugin)) from e
        if isinstance(plugin, type) and issubclass(plugin, Plugin):
            resolved_plugin = plugin()
        elif isinstance(plugin, Plugin):
            resolved_plugin = plugin
        else:
            raise TypeError(
                f"Plugin {plugin} is not a subclass of Plugin, nor a Plugin instance, nor a plugin"
                " name string"
            )
        return resolved_plugin

    def setup_plugins(self, plugins: TPluginArg) -> None:
        if not plugins:
            return
        if not isinstance(plugins, Iterable):
            plugins = [plugins]
        for p in plugins:
            resolved_plugin = self._resolve_plugin(p)
            self._plugins.append(resolved_plugin)
            if isinstance(resolved_plugin, CallbackPlugin):
                self._callback_plugins.append(resolved_plugin)

    def on_step_start(self, step: str, pipeline: SupportsPipeline) -> None:
        for p in self._callback_plugins:
            p.on_step_start(step, pipeline)

    def on_step_end(self, step: str, pipeline: SupportsPipeline) -> None:
        for p in self._callback_plugins:
            p.on_step_end(step, pipeline)

    #
    # callback interfaces
    #

    #
    # contracts callbacks
    #
    def on_schema_contract_violation(
        self,
        error: DataValidationError,
        **kwargs: Any,
    ) -> None:
        for p in self._callback_plugins:
            p.on_schema_contract_violation(error, **kwargs)
