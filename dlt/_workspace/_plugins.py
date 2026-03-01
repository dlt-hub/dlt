import os
from typing import Any, Dict, NamedTuple, Optional, Sequence, Set

from dlt.common.configuration import plugins as _plugins
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase
from dlt.common.runtime.run_context import DOT_DLT, RunContext


class McpFeatures(NamedTuple):
    name: str
    tools: Sequence[Any] = ()
    prompts: Sequence[Any] = ()
    providers: Sequence[Any] = ()


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


@_plugins.hookimpl(specname="plug_mcp")
def plug_mcp_pipeline(features: Set[str]) -> Optional[McpFeatures]:
    """Contribute pipeline-scoped tools: table inspection, SQL queries.

    Activated by the "pipeline" feature. Used by both WorkspaceMCP (features=
    {"workspace", "pipeline"}) and PipelineMCP (features={"pipeline"}).
    """
    if "pipeline" not in features:
        return None

    from dlt._workspace.mcp.tools import data_tools
    from dlt._workspace.mcp import prompts

    tools_list = [t for t in data_tools.__tools__ if t is not data_tools.list_pipelines]

    return McpFeatures(
        name="pipeline",
        tools=tools_list,
        prompts=list(prompts.pipeline.__prompts__),
    )


@_plugins.hookimpl(specname="plug_mcp")
def plug_mcp_workspace(features: Set[str]) -> Optional[McpFeatures]:
    """Contribute workspace-level tools: pipeline discovery.

    Activated by the "workspace" feature. Used by WorkspaceMCP only (features=
    {"workspace", "pipeline"}) so users can list available pipelines.
    """
    if "workspace" not in features:
        return None

    from dlt._workspace.mcp.tools import data_tools

    return McpFeatures(
        name="workspace",
        tools=[data_tools.list_pipelines, data_tools.get_workspace_info],
    )


@_plugins.hookimpl(specname="plug_mcp")
def plug_mcp_toolkit(features: Set[str]) -> Optional[McpFeatures]:
    """Contribute toolkit discovery tools: list and inspect available toolkits.

    Activated by the "toolkit" feature.
    """
    if "toolkit" not in features:
        return None

    from dlt._workspace.mcp.tools import toolkit_tools

    return McpFeatures(name="toolkit", tools=list(toolkit_tools.__tools__))


@_plugins.hookimpl(specname="plug_mcp")
def plug_mcp_secrets(features: Set[str]) -> Optional[McpFeatures]:
    """Contribute secrets management tools: list, view-redacted, update.

    Activated by the "secrets" feature.
    """
    if "secrets" not in features:
        return None

    from dlt._workspace.mcp.tools import secrets_tools

    return McpFeatures(name="secrets", tools=list(secrets_tools.__tools__))


__all__ = [
    "plug_workspace_context_impl",
    "plug_mcp_pipeline",
    "plug_mcp_workspace",
    "plug_mcp_toolkit",
    "plug_mcp_secrets",
]
