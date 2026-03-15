import os
from typing import Any, Dict

from dlt.common.configuration import resolve_configuration

from dlt._workspace import known_sections as ws_known_sections
from dlt._workspace.deployment.configuration import MarimoConfiguration
from dlt._workspace.deployment.launchers._launcher import (
    get_run_args_base_path,
    get_run_args_port,
    parse_launcher_args,
    resolve_module_path,
    set_config_env_vars,
)
from dlt._workspace.deployment.typing import TRuntimeEntryPoint


def run(entry_point: TRuntimeEntryPoint, config: Dict[str, Any]) -> None:
    """Resolve config and exec marimo run."""
    module_name = entry_point["module"]
    section = module_name.rsplit(".", 1)[-1]
    sections = (ws_known_sections.JOBS, section)
    set_config_env_vars(sections, config)

    port = get_run_args_port(entry_point)
    base_path = get_run_args_base_path(entry_point)
    mc = resolve_configuration(MarimoConfiguration(), sections=sections)
    script_path = resolve_module_path(module_name)

    args = ["uv", "run", "marimo", "run", script_path]
    args.extend(["--port", str(port)])
    args.extend(["--host", "0.0.0.0"])
    args.append("--headless")
    args.append("--no-token" if mc.token is None else "--token")
    if mc.token is not None:
        args.extend(["--token-password", mc.token])
    if mc.include_code:
        args.append("--include-code")
    args.extend(["--session-ttl", str(mc.session_ttl)])
    if base_path:
        args.extend(["--base-url", base_path])

    os.execvp("uv", args)


if __name__ == "__main__":
    args = parse_launcher_args()
    run(args.entry_point, args.config)
