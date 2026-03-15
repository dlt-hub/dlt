"""Launcher for plain Python modules via __main__."""

import os
from typing import Dict, Any

from dlt._workspace.deployment.launchers._launcher import (
    parse_launcher_args,
    set_config_env_vars,
)
from dlt._workspace import known_sections as ws_known_sections


def run(module_name: str, config: Dict[str, Any]) -> None:
    """Replace current process with uv run python -m <module_name>."""
    section = module_name.rsplit(".", 1)[-1]
    set_config_env_vars((ws_known_sections.JOBS, section), config)
    os.execvp("uv", ["uv", "run", "python", "-m", module_name])


if __name__ == "__main__":
    args = parse_launcher_args()
    module_name = args.entry_point["module"]
    run(module_name, args.config)
