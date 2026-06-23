"""Job launchers for different framework types."""

LAUNCHER_DASHBOARD = "dlt._workspace.deployment.launchers.dashboard"
LAUNCHER_MARIMO = "dlt._workspace.deployment.launchers.marimo"
LAUNCHER_MCP = "dlt._workspace.deployment.launchers.mcp"
LAUNCHER_STREAMLIT = "dlt._workspace.deployment.launchers.streamlit"
LAUNCHER_JOB = "dlt._workspace.deployment.launchers.job"
LAUNCHER_MODULE = "dlt._workspace.deployment.launchers.module"

_FRAMEWORK_LAUNCHERS = {
    "marimo": LAUNCHER_MARIMO,
    "fastmcp": LAUNCHER_MCP,
    "streamlit": LAUNCHER_STREAMLIT,
}


def get_launcher_for_framework(framework: str) -> str:
    """Get the fully qualified launcher module path for a framework.

    Args:
        framework (str): Framework identifier (`"marimo"`, `"fastmcp"`, `"streamlit"`).

    Raises:
        NotImplementedError: If the framework is not supported.
    """
    launcher = _FRAMEWORK_LAUNCHERS.get(framework)
    if launcher is None:
        raise NotImplementedError(f"no launcher for framework {framework!r}")
    return launcher
