"""Launcher for plain Python modules via __main__."""

import os
from typing import Any, Dict, Optional

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace.deployment.launchers._launcher import (
    parse_launcher_args,
    set_config_env_vars,
)
from dlt._workspace import known_sections as ws_known_sections
from dlt._workspace.deployment.typing import TRuntimeEntryPoint


def run(entry_point: TRuntimeEntryPoint) -> None:
    """Replace current process with uv run python -m <module_name>."""
    module_name = entry_point["module"]
    section = module_name.rsplit(".", 1)[-1]
    set_config_env_vars((ws_known_sections.JOBS, section), entry_point.get("config", {}))
    profile = entry_point.get("profile")
    if profile:
        os.environ[WORKSPACE__PROFILE] = profile
    os.execvp("uv", ["uv", "run", "python", "-m", module_name])


if __name__ == "__main__":
    args = parse_launcher_args()
    run(args.entry_point)
