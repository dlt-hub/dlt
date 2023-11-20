from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Set, Type, TypedDict, NewType, Union, get_args


TExecInfoNames = Literal["kubernetes", "docker", "codespaces", "github_actions", "airflow", "notebook", "colab","aws_lambda","gcp_cloud_function"]

class TVersion(TypedDict):
    """TypeDict representing a library version"""
    name: str
    version: str

class TExecutionContext(TypedDict):
    """TypeDict representing the runtime context info"""
    ci_run: bool
    python: str
    cpu: int
    exec_info: List[TExecInfoNames]
    library: TVersion
    os: TVersion