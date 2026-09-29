import argparse
import importlib.util
import os
import subprocess
from typing import Any, Dict, List, Optional, Tuple

from dlt.common import json
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.utils import add_config_dict_to_env
from dlt.common.time import set_context_timezone

from dlt._workspace import known_sections as ws_known_sections
from dlt.common import known_env
from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace.deployment._job_ref import parse_job_ref
from dlt._workspace.deployment.configuration import JobConfiguration
from dlt._workspace.deployment.typing import TRuntimeEntryPoint, resolve_incremental_mode


def exec_process(argv: List[str]) -> None:
    """Replace the current process with `argv` on POSIX; spawn + wait on Windows."""
    if os.name == "posix":
        os.execvp(argv[0], argv)
    # Windows: os.execvp spawns a detached child and returns 0 to the parent
    # shell, breaking exit-code propagation. Spawn + wait + propagate instead.
    result = subprocess.run(argv)
    raise SystemExit(result.returncode)


def parse_launcher_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse the standard launcher command line.

    All launchers share the same CLI interface:
        python -m dlt._workspace.deployment.launchers.<name> \\
            --run-id <uuid> \\
            --trigger <trigger_string> \\
            --entry-point <json_TRuntimeEntryPoint>
    """
    parser = argparse.ArgumentParser(
        description="dlt job launcher",
    )
    parser.add_argument("--run-id", required=True, help="unique run identifier")
    parser.add_argument("--trigger", required=True, help="trigger string that fired")
    parser.add_argument(
        "--entry-point",
        required=True,
        help="JSON-serialized TRuntimeEntryPoint dict",
    )
    args = parser.parse_args(argv)
    args.entry_point = json.loads(args.entry_point)
    return args


def get_run_args_port(entry_point: TRuntimeEntryPoint) -> int:
    """Extract port from run_args. Raises if not provided."""
    run_args = entry_point.get("run_args", {})
    port = run_args.get("port")
    if port is None:
        raise ValueError(
            "runtime must supply port via run_args. Entry point is missing run_args.port"
        )
    return port


def get_run_args_base_path(entry_point: TRuntimeEntryPoint) -> str:
    """Extract base_path from run_args. Returns empty string if not set."""
    return entry_point.get("run_args", {}).get("base_path", "")


def resolve_module_path(module_name: str) -> str:
    """Resolve a Python module name to its file path without importing it."""
    spec = importlib.util.find_spec(module_name)
    if spec is None:
        raise ValueError(f"module {module_name!r} could not be resolved")

    file_path: Optional[str] = spec.origin
    if file_path is None:
        raise ValueError(f"module {module_name!r} has no origin")
    return file_path


def set_config_env_vars(sections: Tuple[str, ...], config: Dict[str, Any]) -> None:
    """Set config params as env vars using EnvironProvider naming convention."""
    if config:
        add_config_dict_to_env(config, sections, overwrite_keys=True)


def apply_job_configuration(entry_point: TRuntimeEntryPoint) -> None:
    """Updates unset job behavior settings in `entry_point` from configuration.

    Explicit entry point values take precedence over config providers.
    """
    # config resolves in the job's own sections, `jobs.<section>.<name>` taken from `job_ref`.
    parts = tuple(p for p in parse_job_ref(entry_point["job_ref"]) if p)
    sections = (ws_known_sections.JOBS,) + parts
    explicit: Dict[str, Any] = {}
    if entry_point.get("incremental_mode"):
        explicit["incremental_mode"] = entry_point["incremental_mode"]
    if entry_point.get("auto_refresh_pipeline_mode"):
        explicit["auto_refresh_pipeline_mode"] = entry_point["auto_refresh_pipeline_mode"]
    config = resolve_configuration(
        JobConfiguration(), sections=sections, explicit_value=explicit or None
    )
    if config.incremental_mode:
        entry_point["incremental_mode"] = config.incremental_mode
    if config.auto_refresh_pipeline_mode:
        entry_point["auto_refresh_pipeline_mode"] = config.auto_refresh_pipeline_mode


def prepare_run_env(entry_point: TRuntimeEntryPoint) -> None:
    """Set profile, interval and pipeline refresh env vars, before user code runs."""
    profile = entry_point.get("profile")
    if profile:
        os.environ[WORKSPACE__PROFILE] = profile

    iv_start = entry_point.get("interval_start")
    iv_end = entry_point.get("interval_end")
    if iv_start and iv_end:
        os.environ[known_env.DLT_INTERVAL_START] = iv_start
        os.environ[known_env.DLT_INTERVAL_END] = iv_end

    # subprocess launchers exec the user module, so the join decision only reaches incrementals
    # through the environment
    if "incremental_mode" in entry_point:
        os.environ[known_env.DLT_ALLOW_EXTERNAL_SCHEDULERS] = str(
            resolve_incremental_mode(entry_point) == "interval"
        )
    else:
        os.environ.pop(known_env.DLT_ALLOW_EXTERNAL_SCHEDULERS, None)

    # `require.timezone` holds for the whole run, so a manually or on-success triggered job gets
    # it without any interval
    os.environ[known_env.DLT_INTERVAL_TIMEZONE] = entry_point.get("interval_timezone", "UTC")
    # this interpreter read the env at import, so re-read what was just set
    set_context_timezone(None)

    if entry_point.get("refresh") and entry_point.get("auto_refresh_pipeline_mode"):
        set_config_env_vars(
            (known_sections.PIPELINES,), {"refresh": entry_point["auto_refresh_pipeline_mode"]}
        )
