import argparse
import os
from typing import Dict, List, Optional, Tuple

from dlt.common import json

from dlt._workspace.deployment.typing import TRuntimeEntryPoint


def parse_launcher_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse the standard launcher command line.

    All launchers share the same CLI interface:
        uv run python -m dlt._workspace.deployment.launchers.<name> \\
            --run-id <uuid> \\
            --trigger <trigger_string> \\
            --entry-point <json_TRuntimeEntryPoint> \\
            [--config KEY=VALUE ...]
    """
    parser = argparse.ArgumentParser(
        description="dlt job launcher",
    )
    parser.add_argument("--run-id", required=True, help="unique run identifier")
    parser.add_argument("--trigger", required=True, help="trigger string that fired")
    parser.add_argument(
        "--entry-point",
        required=True,
        help="JSON-serialized TRuntimeEntryPoint dict",
    )
    parser.add_argument(
        "--config",
        nargs="*",
        default=[],
        metavar="KEY=VALUE",
        help="config key-value pairs",
    )
    args = parser.parse_args(argv)
    args.entry_point = json.loads(args.entry_point)
    args.config = _parse_config_pairs(args.config)
    return args


def _parse_config_pairs(pairs: List[str]) -> Dict[str, str]:
    """Parse KEY=VALUE pairs from command line."""
    config: Dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"config must be KEY=VALUE, got: {pair!r}")
        key, value = pair.split("=", 1)
        config[key] = value
    return config


def get_run_args_port(entry_point: TRuntimeEntryPoint) -> int:
    """Extract port from run_args. Raises if not provided."""
    run_args = entry_point.get("run_args", {})
    port = run_args.get("port")
    if port is None:
        raise ValueError(
            "runtime must supply port via run_args. Entry point is missing run_args.port"
        )
    return port


def get_run_args_base_path(entry_point: TRuntimeEntryPoint) -> str:
    """Extract base_path from run_args. Returns empty string if not set."""
    return entry_point.get("run_args", {}).get("base_path", "")


def resolve_module_path(module_name: str) -> str:
    """Resolve a Python module name to its file path."""
    from importlib import import_module

    mod = import_module(module_name)
    file_path: Optional[str] = getattr(mod, "__file__", None)
    if file_path is None:
        raise ValueError(f"module {module_name!r} has no __file__")
    return file_path


def set_config_env_vars(sections: Tuple[str, ...], config: Dict[str, str]) -> None:
    """Set config params as env vars with dlt naming convention."""
    if not config:
        return
    prefix = "__".join(sections).upper()
    for key, value in config.items():
        os.environ[f"{prefix}__{key.upper()}"] = value
