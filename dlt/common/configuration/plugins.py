import os
from typing import Any, ClassVar, Dict, List, Optional, Protocol
import pluggy
import argparse
import importlib.metadata

from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase
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

        # take hookspecs from self
        from dlt.common.configuration import plugins

        self.manager.add_hookspecs(plugins)

        # NOTE: internal implementations (__plugins__.py) are declared as entrypoints in pyproject.toml
        self.plugin_modules = load_setuptools_entrypoints(self.manager)


def manager() -> pluggy.PluginManager:
    """Returns current plugin context"""
    from .container import Container

    return Container()[PluginContext].manager


def load_setuptools_entrypoints(m: pluggy.PluginManager) -> List[str]:
    """Scans setuptools distributions that are path or have name starting with `dlt`
    loads entry points in group `dlt` and instantiates them to initialize plugins.

    returns a list of names of top level modules/packages from detected entry points.
    """

    plugin_modules = []

    if os.environ.get(DLT_DISABLE_PLUGINS, "False").lower() == "false":
        distributions = list(importlib.metadata.distributions())
    else:
        # always plug itself
        distributions = [importlib.metadata.distribution("dlt")]

    for dist in distributions:
        # skip named dists that do not start with dlt-
        package_name = dist.metadata.get("Name")

        if not package_name or not package_name.startswith("dlt"):
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


@hookspec(firstresult=True)
def plug_run_context(
    run_dir: Optional[str], runtime_kwargs: Optional[Dict[str, Any]]
) -> Optional[RunContextBase]:
    """Spec for plugin hook that returns current run context.

    Args:
        run_dir (str): An initial run directory of the context
        runtime_kwargs: Any additional arguments passed to the context via PluggableRunContext.reload

    Returns:
        SupportsRunContext: A run context implementing SupportsRunContext protocol
    """


class SupportsCliCommand(Protocol):
    """Protocol for defining one dlt cli command"""

    command: str
    """name of the command"""
    help_string: str
    """the help string for argparse"""
    description: Optional[str]
    """the more detailed description for argparse, may inlcude markdown for the docs"""
    docs_url: Optional[str]
    """the default docs url to be printed in case of an exception"""

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        """Configures the parser for the given argument"""
        ...

    def execute(self, args: argparse.Namespace) -> None:
        """Executes the command with the given arguments"""
        ...


@hookspec()
def plug_cli() -> SupportsCliCommand:
    """Spec for plugin hook that returns current run context."""
