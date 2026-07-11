import sys
from importlib import import_module
from types import ModuleType
from typing import Any, Dict, Tuple

from dlt.common.configuration import resolve_configuration
from dlt.common.libs import is_instance_lib

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
    if "fastmcp" not in sys.modules:
        raise ImportError("fastmcp is not installed. Install it with: pip install fastmcp")

    for name in ("mcp", "server", "app"):
        obj = module.__dict__.get(name)
        if obj is not None and is_instance_lib(obj, class_ref="fastmcp.FastMCP"):
            return obj

    for name, obj in module.__dict__.items():
        if name.startswith("_"):
            continue
        if is_instance_lib(obj, class_ref="fastmcp.FastMCP"):
            return obj

    raise RuntimeError(
        f"no FastMCP instance found in module {module.__name__!r}. "
        "Expected a module-level variable (e.g. mcp = FastMCP(...))"
    )


def _resolve_config(sections: Tuple[str, ...]) -> McpConfiguration:
    """Resolve MCP configuration from the config providers."""
    return resolve_configuration(McpConfiguration(), sections=sections)


def _fastmcp_supports_host_origin_protection() -> bool:
    """True if the installed FastMCP accepts the `host_origin_protection` run arg.

    The DNS-rebinding guard (and this option) landed in FastMCP 3.4.3. Passing the
    argument to an older FastMCP would raise `TypeError`, so gate on the version.
    """
    import semver
    from importlib.metadata import version

    try:
        return semver.Version.parse(version("fastmcp")) >= semver.Version.parse("3.4.3")
    except Exception:
        return False


def run_mcp_instance(instance: Any, port: int, sections: Tuple[str, ...]) -> None:
    """Run a FastMCP instance with resolved configuration.

    Shared entry point for both the MCP launcher (module-level detection)
    and the job launcher (return value fallback).
    """
    config = _resolve_config(sections)
    run_kwargs: Dict[str, Any] = dict(
        transport=config.transport,
        host="0.0.0.0",
        port=port,
        path=config.path,
        log_level=config.log_level,
        stateless_http=config.stateless_http,
    )
    # FastMCP >= 3.4.3 enables DNS-rebinding (Host header) protection by default. Behind
    # the runtime's reverse proxy (modal / tower / local runner), which rewrites the Host
    # header, that guard rejects every request with 421. These servers only receive traffic
    # from the authenticated runtime proxy, so the guard is redundant; disable it by default
    # (overridable via the `host_origin_protection` config). Older FastMCP has no such option.
    if _fastmcp_supports_host_origin_protection():
        run_kwargs["host_origin_protection"] = config.host_origin_protection
    instance.run(**run_kwargs)


def run(entry_point: TRuntimeEntryPoint) -> None:
    """Import module, find FastMCP instance, and run it."""
    module_name = entry_point["module"]
    section = module_name.rsplit(".", 1)[-1]
    sections = (ws_known_sections.JOBS, section)
    set_config_env_vars(sections, entry_point.get("config", {}))

    port = get_run_args_port(entry_point)
    mod = import_module(module_name)
    instance = _find_fastmcp_instance(mod)
    run_mcp_instance(instance, port, sections)


if __name__ == "__main__":
    args = parse_launcher_args()
    run(args.entry_point)
