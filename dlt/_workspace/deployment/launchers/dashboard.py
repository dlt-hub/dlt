"""Launcher for the workspace dashboard (built-in marimo notebook)."""

from dlt._workspace.deployment.launchers._launcher import parse_launcher_args
from dlt._workspace.deployment.launchers.marimo import run as run_marimo
from dlt._workspace.deployment.typing import TRuntimeEntryPoint


def run(entry_point: TRuntimeEntryPoint) -> None:
    """Launch the workspace dashboard via marimo exec (no nested subprocess)."""
    run_marimo(entry_point)


if __name__ == "__main__":
    args = parse_launcher_args()
    run(args.entry_point)
