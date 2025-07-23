import os
from typing import ClassVar, List
import pluggy
import importlib.metadata

from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext
from dlt.common.known_env import DLT_DISABLE_PLUGINS

hookspec = pluggy.HookspecMarker("dlt")
hookimpl = pluggy.HookimplMarker("dlt")


class PluginContext(ContainerInjectableContext):
    global_affinity: ClassVar[bool] = True

    manager: pluggy.PluginManager
    plugin_modules: List[str]

    def __init__(self) -> None:
        super().__init__()
        self.manager = pluggy.PluginManager("dlt")
        self.plugin_modules = []
        if os.environ.get(DLT_DISABLE_PLUGINS, "False").lower() == "true":
            return

        # TODO: we need to solve circular deps somehow

        # run_context
        from dlt.common.runtime import run_context

        self.manager.add_hookspecs(run_context)
        self.manager.register(run_context)

        # cli
        from dlt.cli import plugins

        self.manager.add_hookspecs(plugins)
        self.manager.register(plugins)

        self.plugin_modules = load_setuptools_entrypoints(self.manager)


def manager() -> pluggy.PluginManager:
    """Returns current plugin context"""
    from .container import Container

    return Container()[PluginContext].manager


def load_setuptools_entrypoints(m: pluggy.PluginManager) -> List[str]:
    """Scans setuptools distributions that are path or have name starting with `dlt-`
    loads entry points in group `dlt` and instantiates them to initialize plugins.

    returns a list of names of top level modules/packages from detected entry points.
    """

    plugin_modules = []

    for dist in list(importlib.metadata.distributions()):
        # skip named dists that do not start with dlt-
        package_name = dist.metadata.get("Name")
        if not package_name or not package_name.startswith("dlt-"):
            continue

        for ep in dist.entry_points:
            if (
                ep.group != "dlt"
                # already registered
                or m.get_plugin(ep.name)
                or m.is_blocked(ep.name)
            ):
                continue
            plugin = ep.load()
            m.register(plugin, name=ep.name)
            m._plugin_distinfo.append((plugin, pluggy._manager.DistFacade(dist)))
            top_module = ep.module.split(".")[0]
            if top_module not in plugin_modules:
                plugin_modules.append(top_module)

    return plugin_modules
