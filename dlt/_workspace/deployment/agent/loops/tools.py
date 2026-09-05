"""What a loop needs to run tools: the child environment and the workspace MCP server."""

import os
import sys
import tempfile
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any, Dict, List, Tuple, cast

from dlt._workspace.cli.dlthub.ai.utils import mcp_stdio_args
from dlt._workspace.typing import TWorkspaceAccess, TWorkspaceLocalVerb

MCP_SERVER_ID = "dlt-workspace-mcp"
"""Name the loops give the workspace MCP server, also used by `dlthub ai mcp install`."""

SECRET_FILE_PATTERNS = ("*secrets.toml", ".env", ".env.*")
"""Credential files no file tool opens: dlt's `[<profile>.]secrets.toml` and dotenv files."""

FILE_TOOLS = (
    "Read",
    "NotebookRead",
    "Glob",
    "Grep",
    "Write",
    "Edit",
    "MultiEdit",
    "NotebookEdit",
)
"""Tools that access the filesystem by path, and so take a deny rule each."""

LOCAL_TOOLS: Dict[str, Tuple[str, ...]] = {
    "read": ("Read", "Glob", "Grep"),
    "write": ("Write", "Edit"),
    "execute": ("Bash", "RunPython"),
    "network": ("WebFetch", "WebSearch"),
}
"""What each `access.local` verb buys, named as the Claude Code CLI names its tools."""

LOCAL_TOOL_VERBS: Dict[str, TWorkspaceLocalVerb] = {
    name: cast(TWorkspaceLocalVerb, verb) for verb, names in LOCAL_TOOLS.items() for name in names
}
"""The verb each local tool belongs to."""


def is_secret_file(path: str) -> bool:
    """True when a path names a credential file, wherever in the workspace it sits."""
    name = os.path.basename(path)
    return any(fnmatchcase(name, pattern) for pattern in SECRET_FILE_PATTERNS)


def secret_deny_rules() -> List[str]:
    """CLI rules that keep its file tools out of credential files."""
    return [f"{tool}(**/{pattern})" for tool in FILE_TOOLS for pattern in SECRET_FILE_PATTERNS]


def cli_host_command() -> str:
    """The `dlthub` script from the environment the job runs in. `ai` exists on no other host."""
    script = Path(sys.executable).parent / ("dlthub.exe" if os.name == "nt" else "dlthub")
    return str(script) if script.is_file() else "dlthub"


def tool_env() -> Dict[str, str]:
    """Child environment with the job's virtualenv on PATH, so `dlthub`, `dlt` and `python` resolve.

    The launcher may be started as `<venv>/bin/python -m ...`, which puts nothing on PATH.
    """
    bin_dir = Path(sys.executable).parent
    env = dict(os.environ)
    if str(bin_dir) not in env.get("PATH", "").split(os.pathsep):
        env["PATH"] = os.pathsep.join(filter(None, [str(bin_dir), env.get("PATH", "")]))
    if (bin_dir.parent / "pyvenv.cfg").is_file():
        env["VIRTUAL_ENV"] = str(bin_dir.parent)
    # the MCP server we spawn writes to our stderr, so it keeps quiet
    env["FASTMCP_SHOW_SERVER_BANNER"] = "false"
    env["FASTMCP_LOG_LEVEL"] = "WARNING"
    return env


def temp_dir() -> Path:
    """The system temp folder, where an agent may keep scratch files outside the workspace."""
    return Path(tempfile.gettempdir()).resolve()


def mcp_server_command(tools: List[str], access: TWorkspaceAccess) -> Dict[str, Any]:
    """Stdio server config serving the feature groups an agent declared, limited to its access.

    The server reads `WORKSPACE__PROFILE` and the workspace config from the environment.
    """
    return {
        "type": "stdio",
        "command": cli_host_command(),
        "args": mcp_stdio_args(tools, with_defaults=False, access=access or {}),
        "env": tool_env(),
    }
