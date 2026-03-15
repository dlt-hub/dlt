"""Full workspace deployment with all job types."""

__tags__ = ["production", "team:data"]

from tests.workspace.cases.runtime_workspace.batch_jobs import (
    backfill,
    daily_ingest,
    transform,
    maintenance,
)
from tests.workspace.cases.runtime_workspace.interactive_jobs import (
    api_server,
    mcp_tools,
)
import tests.workspace.cases.runtime_workspace.marimo_notebook as marimo_notebook
import tests.workspace.cases.runtime_workspace.mcp_server as mcp_server
import tests.workspace.cases.runtime_workspace.streamlit_app as streamlit_app

__all__ = [
    "backfill",
    "daily_ingest",
    "transform",
    "maintenance",
    "api_server",
    "mcp_tools",
    "marimo_notebook",
    "mcp_server",
    "streamlit_app",
]
