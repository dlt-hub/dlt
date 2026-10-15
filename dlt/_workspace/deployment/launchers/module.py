"""Launcher for plain Python modules via __main__."""

import sys
from typing import Any, Dict, Optional

from dlt._workspace.deployment.launchers._launcher import (
    apply_job_configuration,
    exec_process,
    parse_launcher_args,
    prepare_run_env,
    set_config_env_vars,
)
from dlt._workspace import known_sections as ws_known_sections
from dlt._workspace.deployment.typing import TRuntimeEntryPoint


def run(entry_point: TRuntimeEntryPoint) -> None:
    """Replace current process with python -m <module_name>."""
    module_name = entry_point["module"]
    section = module_name.rsplit(".", 1)[-1]
    # exec'd process inherits the env
    apply_job_configuration(entry_point)
    prepare_run_env(entry_point)
    set_config_env_vars((ws_known_sections.JOBS, section), entry_point.get("config", {}))
    exec_process([sys.executable, "-m", module_name])


if __name__ == "__main__":
    args = parse_launcher_args()
    run(args.entry_point)
