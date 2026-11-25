"""Launcher for the workspace dashboard (built-in marimo notebook)."""

import dlt
from dlt.common import logger

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli.utils import list_local_pipelines
from dlt._workspace.deployment.launchers._launcher import (
    activate_profile,
    apply_job_configuration,
    get_run_args_port,
    parse_launcher_args,
    prepare_run_env,
)
from dlt._workspace.deployment.typing import TRuntimeEntryPoint
from dlt._workspace.helpers.dashboard.runner import run_dashboard
from dlt._workspace.helpers.dashboard.utils.pipeline import (
    get_dashboard_pipelines_dir,
    get_runtime_pipelines_profile,
    resolve_dashboard_config,
)
from dlt._workspace.helpers.runtime.runtime_artifacts import sync_from_runtime


def run(entry_point: TRuntimeEntryPoint) -> None:
    """Launch the workspace dashboard."""
    activate_profile(entry_point)
    apply_job_configuration(entry_point)
    prepare_run_env(entry_point)
    _sync_and_log_runtime_pipelines()
    port = get_run_args_port(entry_point)
    run_dashboard(port=port, host="0.0.0.0", headless=True)


def _sync_and_log_runtime_pipelines() -> None:
    # marimo runs the notebook only when a session connects: log what the dashboard will show
    context = dlt.current.run_context()
    if not isinstance(context, WorkspaceRunContext):
        return
    if not resolve_dashboard_config(None).sync_from_runtime:
        return
    try:
        sync_from_runtime()
    except Exception:
        logger.warning(
            "dlt could not sync the pipelines from the runtime. The dashboard syncs again when a"
            " session connects.",
            exc_info=True,
        )
        return
    pipelines_dir, pipelines, _ = list_local_pipelines(get_dashboard_pipelines_dir(None, True))
    profile = get_runtime_pipelines_profile() or context.profile
    names = ", ".join(p["name"] for p in pipelines) or "none"
    logger.info(
        f"Dashboard shows the pipelines of profile `{profile}` (active profile"
        f" `{context.profile}`) in `{pipelines_dir}`: {names}"
    )


if __name__ == "__main__":
    args = parse_launcher_args()
    run(args.entry_point)
