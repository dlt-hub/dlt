"""Module-level framework detectors for interactive job discovery.

Each detector probes an imported module for a known framework singleton
(marimo.App, FastMCP, streamlit) and produces a TJobDefinition.

`detect_local_module` is a separate detector for plain Python modules
that should run as batch jobs via `__main__`. It validates the module
is local to the workspace (below or equal to the parent module).
"""

import os
from types import ModuleType
from typing import Any, List, Optional, Tuple

from dlt.common.utils import get_module_name

from dlt._workspace.deployment._job_ref import make_job_ref
from dlt._workspace.deployment._trigger_helpers import normalize_triggers
from dlt._workspace.deployment.launchers import LAUNCHER_MODULE, get_launcher_for_framework
from dlt._workspace.deployment import trigger as _triggers
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecuteSpec,
    TExposeSpec,
    TJobDefinition,
    TJobRef,
    TTrigger,
)

_HTTP_TRIGGER = _triggers.http()
_INTERACTIVE_EXECUTION = TExecuteSpec(concurrency=1)


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


def _apply_module_dunders(module: ModuleType, job_def: TJobDefinition) -> None:
    """Apply module-level dunders to a detected job definition.

    Reads `__trigger__`, `__expose__`, and `__requires__` from the module and
    merges them into the job definition produced by a detector.
    """
    # __trigger__: append to detector triggers
    raw_trigger = getattr(module, "__trigger__", None)
    if raw_trigger is not None:
        extra: List[TTrigger] = normalize_triggers(raw_trigger)
        job_def["triggers"].extend(extra)

    # __expose__: override detector expose
    expose = getattr(module, "__expose__", None)
    if expose is not None:
        job_def["expose"] = expose

    # __requires__: set job requirements
    requires = getattr(module, "__requires__", None)
    if requires is not None:
        job_def["require"] = requires


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
        "expose": TExposeSpec(interface="gui", category="notebook", tags=["notebook"]),
        "triggers": [_HTTP_TRIGGER],
        "execute": _INTERACTIVE_EXECUTION,
    }
    description = _module_description(module)
    if display_name and description:
        job_def["description"] = f"{display_name}: {description}"
    elif display_name:
        job_def["description"] = display_name
    elif description:
        job_def["description"] = description

    _apply_module_dunders(module, job_def)
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
        "expose": TExposeSpec(interface="mcp", category="mcp"),
        "triggers": [_HTTP_TRIGGER],
        "execute": _INTERACTIVE_EXECUTION,
    }
    description = _module_description(module)
    if server_name and description:
        job_def["description"] = f"{server_name}: {description}"
    elif server_name:
        job_def["description"] = server_name
    elif description:
        job_def["description"] = description

    _apply_module_dunders(module, job_def)
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
        "expose": TExposeSpec(interface="gui", category="dashboard"),
        "triggers": [_HTTP_TRIGGER],
        "execute": _INTERACTIVE_EXECUTION,
    }
    description = _module_description(module)
    if description:
        job_def["description"] = description

    _apply_module_dunders(module, job_def)
    return job_def


_VENV_PATH_MARKERS = ("site-packages", ".venv", "venv", ".tox", ".nox")


def is_local_module(module: ModuleType, parent_module: ModuleType) -> bool:
    """Check if module's file is below parent_module's directory and not in a venv."""
    module_file = getattr(module, "__file__", None)
    parent_file = getattr(parent_module, "__file__", None)
    if module_file is None or parent_file is None:
        return False

    parent_dir = os.path.dirname(os.path.realpath(parent_file))
    module_path = os.path.realpath(module_file)

    # reject installed packages (venv, site-packages)
    path_parts = module_path.split(os.sep)
    if any(marker in path_parts for marker in _VENV_PATH_MARKERS):
        return False

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
        "launcher": LAUNCHER_MODULE,
    }
    job_def: TJobDefinition = {
        "job_ref": _module_job_ref(module),
        "entry_point": entry_point,
        "triggers": [],
        "execute": TExecuteSpec(),
    }
    description = _module_description(module)
    if description:
        job_def["description"] = description

    _apply_module_dunders(module, job_def)
    return job_def
