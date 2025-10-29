import os
from typing import Any, Dict, Optional

from dlt.common.configuration import plugins as _plugins
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase
from dlt.common.runtime.run_context import DOT_DLT, RunContext

__all__ = ["plug_workspace_context_impl"]


def is_workspace_dir(run_dir: str) -> bool:
    """Checks if `run_dir` contains dlt workspace, this is true if a config file is found"""
    return os.path.isfile(os.path.join(run_dir, DOT_DLT, ".workspace"))


@_plugins.hookimpl(specname="plug_run_context", trylast=False)
def plug_workspace_context_impl(
    run_dir: Optional[str], runtime_kwargs: Optional[Dict[str, Any]]
) -> Optional[RunContextBase]:
    # TODO: if recursive search was requested
    # if runtime_kwargs.get("_look_recursive")
    run_dir = os.path.abspath(run_dir or ".")
    if is_workspace_dir(run_dir):
        # import workspace only when context detected
        from dlt._workspace.profile import DEFAULT_PROFILE, read_profile_pin
        from dlt._workspace.run_context import default_name
        from dlt._workspace._workspace_context import WorkspaceRunContext

        profile: str = None
        if runtime_kwargs:
            profile = runtime_kwargs.get("profile")
        profile = profile or read_profile_pin(RunContext(run_dir)) or DEFAULT_PROFILE
        return WorkspaceRunContext(default_name(run_dir), run_dir, profile)
    elif runtime_kwargs and runtime_kwargs.get("_required") == "WorkspaceRunContext":
        from dlt._workspace.exceptions import WorkspaceRunContextNotAvailable

        raise WorkspaceRunContextNotAvailable(run_dir)

    return None
