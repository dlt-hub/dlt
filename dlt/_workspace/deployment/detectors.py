"""Module-level framework detectors for interactive job discovery.

Each detector probes an imported module for a known framework singleton
(marimo.App, FastMCP, streamlit) and produces a TJobDefinition.

`detect_local_module` is a separate detector for plain Python modules
that should run as batch jobs via `__main__`. It validates the module
is local to the workspace (below or equal to the parent module).
"""

import os
from types import ModuleType
from typing import Any, Optional, Tuple

from dlt.common.utils import get_module_name

from dlt._workspace.deployment._job_ref import make_job_ref
from dlt._workspace.deployment.launchers import get_launcher_for_framework
from dlt._workspace.deployment import triggers as _triggers
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecutionSpec,
    TExposeSpec,
    TJobDefinition,
    TJobRef,
)

_HTTP_TRIGGER = _triggers.http()
_INTERACTIVE_EXECUTION = TExecutionSpec(concurrency=1, timeout={"grace_period": 5.0})


def detect_module_job(module: ModuleType) -> Optional[TJobDefinition]:
    """Detects if `module` may be a job by running frameworks detectors"""
    for detector in (_detect_marimo, _detect_mcp, _detect_streamlit):
        result = detector(module)
        if result is not None:
            return result
    return None


def _find_instance(
    module: ModuleType,
    type_class: type,
    preferred_names: Tuple[str, ...] = (),
) -> Optional[Tuple[str, Any]]:
    """Find an instance of type_class in module namespace."""
    for name in preferred_names:
        obj = module.__dict__.get(name)
        if obj is not None and isinstance(obj, type_class):
            return name, obj

    for name, obj in module.__dict__.items():
        if name.startswith("_"):
            continue
        if isinstance(obj, type_class):
            return name, obj

    return None


def _module_job_ref(module: ModuleType) -> TJobRef:
    """Build a job_ref for a module-level job: jobs.<module_name>."""
    return make_job_ref("", get_module_name(module))


def _module_description(module: ModuleType) -> Optional[str]:
    """Extract description from module docstring."""
    doc: Optional[str] = getattr(module, "__doc__", None)
    if doc and doc.strip():
        return doc.strip()
    return None


def _detect_marimo(module: ModuleType) -> Optional[TJobDefinition]:
    """Detect a marimo.App instance."""
    try:
        from marimo import App as MarimoApp
    except ImportError:
        return None

    match = _find_instance(module, MarimoApp, ("app",))
    if match is None:
        return None

    _, app = match
    display_name: Optional[str] = None
    config = getattr(app, "_config", None)
    if config is not None:
        display_name = getattr(config, "app_title", None)

    entry_point: TEntryPoint = {
        "module": module.__name__,
        "function": None,
        "job_type": "interactive",
        "launcher": get_launcher_for_framework("marimo"),
    }
    job_def: TJobDefinition = {
        "job_ref": _module_job_ref(module),
        "entry_point": entry_point,
        "expose": TExposeSpec(interface="gui"),
        "triggers": [_HTTP_TRIGGER],
        "execution": _INTERACTIVE_EXECUTION,
        "starred": False,
        "tags": ["notebook"],
    }
    if display_name:
        job_def["display_name"] = display_name

    description = _module_description(module)
    if description:
        job_def["description"] = description

    return job_def


def _detect_mcp(module: ModuleType) -> Optional[TJobDefinition]:
    """Detect a FastMCP instance."""
    try:
        from fastmcp import FastMCP
    except ImportError:
        return None

    match = _find_instance(module, FastMCP, ("mcp", "server", "app"))
    if match is None:
        return None

    _, mcp = match
    server_name = getattr(mcp, "name", None)

    entry_point: TEntryPoint = {
        "module": module.__name__,
        "function": None,
        "job_type": "interactive",
        "launcher": get_launcher_for_framework("fastmcp"),
    }
    job_def: TJobDefinition = {
        "job_ref": _module_job_ref(module),
        "entry_point": entry_point,
        "expose": TExposeSpec(interface="mcp"),
        "triggers": [_HTTP_TRIGGER],
        "execution": _INTERACTIVE_EXECUTION,
        "starred": False,
    }
    if server_name:
        job_def["display_name"] = server_name

    description = _module_description(module)
    if description:
        job_def["description"] = description

    return job_def


def _detect_streamlit(module: ModuleType) -> Optional[TJobDefinition]:
    """Detect streamlit usage by finding the streamlit module in namespace."""
    for obj in module.__dict__.values():
        if isinstance(obj, ModuleType) and getattr(obj, "__name__", "") == "streamlit":
            break
    else:
        return None

    entry_point: TEntryPoint = {
        "module": module.__name__,
        "function": None,
        "job_type": "interactive",
        "launcher": get_launcher_for_framework("streamlit"),
    }
    job_def: TJobDefinition = {
        "job_ref": _module_job_ref(module),
        "entry_point": entry_point,
        "expose": TExposeSpec(interface="gui"),
        "triggers": [_HTTP_TRIGGER],
        "execution": _INTERACTIVE_EXECUTION,
        "starred": False,
        "tags": ["dashboard"],
    }
    description = _module_description(module)
    if description:
        job_def["description"] = description

    return job_def


def is_local_module(module: ModuleType, parent_module: ModuleType) -> bool:
    """Check if module's file is below or equal to parent_module's directory."""
    module_file = getattr(module, "__file__", None)
    parent_file = getattr(parent_module, "__file__", None)
    if module_file is None or parent_file is None:
        return False

    parent_dir = os.path.dirname(os.path.realpath(parent_file))
    module_path = os.path.realpath(module_file)
    try:
        common = os.path.commonpath([parent_dir, module_path])
    except ValueError:
        return False
    return bool(common == parent_dir)


def detect_local_module(module: ModuleType, parent_module: ModuleType) -> Optional[TJobDefinition]:
    """Detect a plain local Python module as a batch job.

    Only matches modules local to the workspace. Skips modules already
    detected by framework detectors. Not part of the framework detection
    chain — called separately.
    """
    if not is_local_module(module, parent_module):
        return None

    if detect_module_job(module) is not None:
        return None

    entry_point: TEntryPoint = {
        "module": module.__name__,
        "function": None,
        "job_type": "batch",
    }
    job_def: TJobDefinition = {
        "job_ref": _module_job_ref(module),
        "entry_point": entry_point,
        "triggers": [],
        "execution": TExecutionSpec(),
        "starred": False,
    }
    description = _module_description(module)
    if description:
        job_def["description"] = description

    return job_def
