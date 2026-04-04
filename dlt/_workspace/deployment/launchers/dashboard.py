"""Launcher for the workspace dashboard (built-in marimo notebook)."""

from dlt._workspace.deployment.launchers._launcher import (
    get_run_args_port,
    parse_launcher_args,
)
from dlt._workspace.deployment.typing import TRuntimeEntryPoint


def run(entry_point: TRuntimeEntryPoint) -> None:
    """Launch the workspace dashboard."""
    from dlt._workspace.helpers.dashboard.runner import run_dashboard

    port = get_run_args_port(entry_point)
    run_dashboard(port=port, host="0.0.0.0", headless=True)


if __name__ == "__main__":
    args = parse_launcher_args()
    run(args.entry_point)
