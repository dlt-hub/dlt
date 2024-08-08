from importlib.metadata import entry_points
from importlib import import_module
from argparse import ArgumentParser, Namespace

from typing import Callable, Set, Optional, Dict, Any, TypedDict


# cli plugin is called once to populate the parser and possible a second time with the received args
TCliPlugin = Callable[[Optional[ArgumentParser], Optional[Namespace]], None]


class CliPlugin(TypedDict):
    func: TCliPlugin
    cli_help: str


# registered plugins
_DLT_PLUGINS: Set[str] = set()
_DLT_CLI_PLUGINS: Dict[str, CliPlugin] = {}
_CURRENT_PLUGIN: Optional[str] = None


def cli_plugin(subcommand: str, cli_help: str) -> Callable[[TCliPlugin], None]:
    """decorator for creating a dlt cli subcommand"""

    def register(cli_plugin: TCliPlugin) -> None:
        # print("Registering cli command")
        assert _CURRENT_PLUGIN
        assert (
            subcommand not in _DLT_CLI_PLUGINS
        ), f"CLI Plugin with name {subcommand} already exists"
        _DLT_CLI_PLUGINS[subcommand] = {"func": cli_plugin, "cli_help": cli_help}

    return register


def discover_plugins() -> None:
    global _CURRENT_PLUGIN
    for entry_point in entry_points().select(group="dlt.plugin"):  # type: ignore
        # print(f"Found dlt plugin {entry_point.name}")
        _CURRENT_PLUGIN = entry_point.name
        import_module(entry_point.value)
        _DLT_PLUGINS.add(entry_point.name)
        _CURRENT_PLUGIN = None
