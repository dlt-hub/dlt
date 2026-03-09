"""Common launcher infrastructure shared by all launcher modules."""

import argparse
import json
from typing import Dict, List, Optional

from dlt._workspace.deployment.typing import TEntryPoint


def parse_launcher_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse the standard launcher command line.

    All launchers share the same CLI interface:
        python -m dlt._workspace.deployment.launchers.<name> \\
            --run-id <uuid> \\
            --trigger <trigger_string> \\
            --entry-point <json_TEntryPoint> \\
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
        help="JSON-serialized TEntryPoint dict",
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
