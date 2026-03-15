import os
from typing import Any, Dict

from dlt.common.configuration import resolve_configuration

from dlt._workspace import known_sections as ws_known_sections
from dlt._workspace.deployment.configuration import StreamlitConfiguration
from dlt._workspace.deployment.launchers._launcher import (
    get_run_args_base_path,
    get_run_args_port,
    parse_launcher_args,
    resolve_module_path,
    set_config_env_vars,
)
from dlt._workspace.deployment.typing import TRuntimeEntryPoint


def run(entry_point: TRuntimeEntryPoint, config: Dict[str, Any]) -> None:
    """Resolve config and exec streamlit run."""
    module_name = entry_point["module"]
    section = module_name.rsplit(".", 1)[-1]
    sections = (ws_known_sections.JOBS, section)
    set_config_env_vars(sections, config)

    port = get_run_args_port(entry_point)
    base_path = get_run_args_base_path(entry_point)
    sc = resolve_configuration(StreamlitConfiguration(), sections=sections)
    script_path = resolve_module_path(module_name)

    args = [
        "uv",
        "run",
        "streamlit",
        "run",
        script_path,
        "--server.address=0.0.0.0",
        f"--server.port={port}",
        "--server.headless=true",
        f"--server.enableCORS={'true' if sc.enable_cors else 'false'}",
        f"--server.enableXsrfProtection={'true' if sc.enable_xsrf_protection else 'false'}",
        "--server.enableWebsocketCompression=false",
        f"--browser.gatherUsageStats={'true' if sc.gather_usage_stats else 'false'}",
        "--browser.serverAddress=0.0.0.0",
        f"--browser.serverPort={port}",
    ]
    if base_path:
        args.append(f"--server.baseUrlPath={base_path}")

    os.execvp("uv", args)


if __name__ == "__main__":
    args = parse_launcher_args()
    run(args.entry_point, args.config)
