from typing import (
    List,
    Literal,
)
from dlt.common.typing import TypedDict

TExecInfoNames = Literal[
    "kubernetes",
    "docker",
    "codespaces",
    "github_actions",
    "airflow",
    "notebook",
    "colab",
    "aws_lambda",
    "gcp_cloud_function",
    "streamlit",
    "dagster",
    "prefect",
    "marimo",
    "claude_code",
    "claude_code_cli",
    "cursor",
    "codex",
]


class TVersion(TypedDict):
    """TypeDict representing a library version"""

    name: str
    version: str


class TRunContextInfo(TypedDict, total=False):
    """TypeDict representing the active run context"""

    name: str
    profile: str
    """Present only if run context supports profiles"""


class TExecutionContext(TypedDict, total=False):
    """TypeDict representing the runtime context info"""

    ci_run: bool
    python: str
    cpu: int
    exec_info: List[TExecInfoNames]
    library: TVersion
    os: TVersion
    run_context: TRunContextInfo
    dlthub: TVersion
