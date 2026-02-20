"""Constants and type aliases for the dashboard."""

from typing import Dict, List, Literal

from typing_extensions import TypeAlias

from dlt.common.storages.load_package import TLoadPackageStatus


# Runner file names

EJECTED_APP_FILE_NAME = "dlt_dashboard.py"
STYLE_FILE_NAME = "dlt_dashboard_styles.css"


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
