"""Constants and type aliases for the dashboard."""

from typing import Dict, List, Literal

from typing_extensions import TypeAlias

from dlt.common.storages.load_package import TLoadPackageStatus


# Runner file names

EJECTED_APP_FILE_NAME = "dlt_dashboard.py"
STYLE_FILE_NAME = "dlt_dashboard_styles.css"

# Runner defaults

DEFAULT_DASHBOARD_PORT = 2718
"""Default port for the marimo dashboard server."""

MARIMO_COMMAND = "marimo"
"""The marimo CLI executable name."""

HTTP_READY_TIMEOUT_S = 15.0
"""Seconds to wait for the dashboard HTTP server to become ready."""

HTTP_READY_POLL_INTERVAL_S = 0.1
"""Seconds between HTTP readiness polls."""

HTTP_READY_WAIT_ON_OK_S = 0.1
"""Extra wait after the server responds OK (default for _wait_http_up)."""

DASHBOARD_STARTUP_TIMEOUT_S = 60.0
"""Seconds to wait for the dashboard process in start_dashboard()."""

DASHBOARD_STARTUP_WAIT_ON_OK_S = 1.0
"""Extra wait after ready in start_dashboard()."""

PROCESS_TERMINATE_TIMEOUT_S = 10
"""Seconds to wait for the dashboard process to terminate before killing it."""


# DashboardConfiguration defaults

DEFAULT_TABLE_LIST_FIELDS: List[str] = [
    "parent",
    "resource",
    "write_disposition",
    "description",
]
"""Default fields shown in table lists (name is always present)."""

DEFAULT_COLUMN_TYPE_HINTS: List[str] = [
    "data_type",
    "nullable",
    "precision",
    "scale",
    "timezone",
    "variant",
]
"""Default column type hints shown in the column list."""

DEFAULT_COLUMN_OTHER_HINTS: List[str] = [
    "primary_key",
    "merge_key",
    "unique",
]
"""Default other column hints shown in the column list."""

DEFAULT_DATETIME_FORMAT = "YYYY-MM-DD HH:mm:ss Z"
"""Default datetime display format."""


# Pipeline execution visualization

TPipelineRunStatus: TypeAlias = Literal["succeeded", "failed"]
TVisualPipelineStep: TypeAlias = Literal["extract", "normalize", "load"]

VISUAL_PIPELINE_STEPS: List[TVisualPipelineStep] = ["extract", "normalize", "load"]

PIPELINE_RUN_STEP_COLORS: Dict[TVisualPipelineStep, str] = {
    "extract": "var(--dlt-color-lime)",
    "normalize": "var(--dlt-color-aqua)",
    "load": "var(--dlt-color-pink)",
}


# Load package statuses

PENDING_LOAD_STATUSES: Dict[TLoadPackageStatus, str] = {
    "extracted": "pending to normalize",
    "normalized": "pending to load",
}

LOAD_PACKAGE_STATUS_COLORS: Dict[TLoadPackageStatus, str] = {
    "new": "grey",
    "extracted": "yellow",
    "normalized": "yellow",
    "loaded": "green",
    "aborted": "red",
}
