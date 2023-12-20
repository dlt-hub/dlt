from typing import Type, Union, List, Any, Callable
from dlt.common.typing import TFun
from dlt.common.typing import TDataItem
from dlt.common.pipeline import SupportsPipeline
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.schema.typing import TSchemaContract
from dlt.common.configuration.specs import configspec
from functools import wraps
from .reference import SupportsCallbackPlugin, Plugin, TSinglePluginArg, TPluginArg
from dlt.common.configuration.container import Container


@configspec
class PluginsContext(ContainerInjectableContext, SupportsCallbackPlugin):
    def __init__(self) -> None:
        self._plugins: List[Plugin] = []
        self.steps: List[str] = []

    def _resolve_plugin(self, plugin: TSinglePluginArg, pipeline: SupportsPipeline) -> Plugin:
        if isinstance(plugin, str):
            pass  # TODO
        elif isinstance(plugin, type) and issubclass(plugin, Plugin):
            plugin = plugin()
        elif isinstance(plugin, Plugin):
            pass
        else:
            raise TypeError(
                f"Plugin {plugin} is not a subclass of Plugin, nor a Plugin instance, nor a plugin"
                " name string"
            )
        return plugin

    def setup_plugins(self, plugins: TPluginArg, pipeline: SupportsPipeline) -> None:
        for p in plugins:
            self._plugins.append(self._resolve_plugin(p, pipeline))

    def on_step_start(self, step: str) -> None:
        self.steps.append(step)
        for p in self._plugins:
            p.step = self.steps[-1]
            p.on_step_start(step)

    def on_step_end(self, step: str) -> None:
        self.steps.pop()
        for p in self._plugins:
            p.on_step_end(step)
            p.step = self.steps[-1] if self.steps else None


def with_plugins() -> Callable[[TFun], TFun]:
    def decorator(f: TFun) -> TFun:
        @wraps(f)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            # get plugins context
            plugins = Container()[PluginsContext]
            return f(*args, **kwargs, _plugins=plugins)

        return _wrap  # type: ignore

    return decorator
