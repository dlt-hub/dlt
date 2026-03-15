import sys
from importlib import import_module
from types import ModuleType
from typing import Any, Dict, Tuple

from dlt.common.configuration import resolve_configuration

from dlt._workspace import known_sections as ws_known_sections
from dlt._workspace.deployment.configuration import McpConfiguration
from dlt._workspace.deployment.launchers._launcher import (
    get_run_args_port,
    parse_launcher_args,
    set_config_env_vars,
)
from dlt._workspace.deployment.typing import TRuntimeEntryPoint


def _find_fastmcp_instance(module: ModuleType) -> Any:
    """Find a FastMCP instance in the module namespace."""
    try:
        from fastmcp import FastMCP
    except ImportError:
        raise ImportError("fastmcp is not installed. Install it with: pip install fastmcp")

    for name in ("mcp", "server", "app"):
        obj = module.__dict__.get(name)
        if obj is not None and isinstance(obj, FastMCP):
            return obj

    for name, obj in module.__dict__.items():
        if name.startswith("_"):
            continue
        if isinstance(obj, FastMCP):
            return obj

    raise RuntimeError(
        f"no FastMCP instance found in module {module.__name__!r}. "
        "Expected a module-level variable (e.g. mcp = FastMCP(...))"
    )


def _resolve_config(sections: Tuple[str, ...]) -> McpConfiguration:
    """Resolve MCP configuration from the config providers."""
    return resolve_configuration(McpConfiguration(), sections=sections)


def run_mcp_instance(instance: Any, port: int, sections: Tuple[str, ...]) -> None:
    """Run a FastMCP instance with resolved configuration.

    Shared entry point for both the MCP launcher (module-level detection)
    and the job launcher (return value fallback).
    """
    config = _resolve_config(sections)
    instance.run(
        transport=config.transport,
        host="0.0.0.0",
        port=port,
        path=config.path,
        log_level=config.log_level,
        stateless_http=config.stateless_http,
    )


def run(entry_point: TRuntimeEntryPoint, config: Dict[str, Any]) -> None:
    """Import module, find FastMCP instance, and run it."""
    module_name = entry_point["module"]
    section = module_name.rsplit(".", 1)[-1]
    sections = (ws_known_sections.JOBS, section)
    set_config_env_vars(sections, config)

    port = get_run_args_port(entry_point)
    mod = import_module(module_name)
    instance = _find_fastmcp_instance(mod)
    run_mcp_instance(instance, port, sections)


if __name__ == "__main__":
    args = parse_launcher_args()
    # let the exception end the process
    run(args.entry_point, args.config)
