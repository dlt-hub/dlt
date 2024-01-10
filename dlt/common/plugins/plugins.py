from typing import List, Any, Iterable, Optional, Dict
from dlt.common.typing import TFun
from dlt.common.typing import TDataItem
from dlt.common.pipeline import SupportsPipeline
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.configuration.specs import configspec
from functools import wraps
from .reference import SupportsCallbackPlugin, Plugin, TSinglePluginArg, TPluginArg, CallbackPlugin
from dlt.common.schema.exceptions import DataValidationError
from importlib import import_module
from .exceptions import UnknownPluginPathException
from dlt.common.configuration.specs.base_configuration import BaseConfiguration
import multiprocessing as mp
from functools import wraps
import threading


def on_main_process(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(self: "PluginsContext", *args: Any, **kwargs: Any) -> Any:
        # send message to shared queue if this is not the main instance
        if not self._main or threading.main_thread() != threading.current_thread():
            self._queue.put((f.__name__, args, kwargs))
            return None
        return f(self, *args, **kwargs)

    return _wrap  # type: ignore


@configspec
class PluginsContext(ContainerInjectableContext, SupportsCallbackPlugin):
    def __init__(self, main: bool = True) -> None:
        self._plugins: List[Plugin[BaseConfiguration]] = []
        self._callback_plugins: List[CallbackPlugin[BaseConfiguration]] = []
        self._initial_plugins: TPluginArg = []
        self._main = main

        if self._main:
            manager = mp.Manager()
            self._queue = manager.Queue()

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
        else:
            raise TypeError(f"Plugin {plugin} is not a subclass of Plugin nor a plugin name string")
        return resolved_plugin

    # pickle support
    def __getstate__(self) -> Dict[str, Any]:
        return {"plugins": self._initial_plugins, "queue": self._queue}

    def __setstate__(self, d: Dict[str, Any]) -> None:
        self.__init__(False)  # type: ignore[misc]
        self.setup_plugins(d["plugins"])
        self._queue = d["queue"]

    def process_queue(self) -> None:
        assert self._main
        try:
            while True:
                name, args, kwargs = self._queue.get_nowait()
                getattr(self, name)(*args, **kwargs)
        except mp.queues.Empty:  # type: ignore
            pass

    def setup_plugins(self, plugins: TPluginArg) -> None:
        self._initial_plugins = plugins
        if not plugins:
            return
        if not isinstance(plugins, Iterable):
            plugins = [plugins]
        for p in plugins:
            resolved_plugin = self._resolve_plugin(p)
            self._plugins.append(resolved_plugin)
            if isinstance(resolved_plugin, CallbackPlugin):
                self._callback_plugins.append(resolved_plugin)

    def get_plugin(self, plugin_name: str) -> Optional[Plugin[BaseConfiguration]]:
        for p in self._plugins:
            if p.NAME == plugin_name:
                return p
        return None

    #
    # Main Pipeline Callbacks
    #
    @on_main_process
    def on_step_start(self, step: str, pipeline: SupportsPipeline) -> None:
        for p in self._callback_plugins:
            p.on_step_start(step, pipeline)

    @on_main_process
    def on_step_end(self, step: str, pipeline: SupportsPipeline) -> None:
        for p in self._callback_plugins:
            p.on_step_end(step, pipeline)

    #
    # contracts callbacks
    #
    @on_main_process
    def on_schema_contract_violation(
        self,
        error: DataValidationError,
        **kwargs: Any,
    ) -> None:
        for p in self._callback_plugins:
            p.on_schema_contract_violation(error, **kwargs)
