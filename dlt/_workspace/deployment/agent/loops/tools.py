"""What a loop needs to run tools: the local tools, their environment and the workspace MCP server."""

import os
import re
import shutil
import subprocess
import sys
import tempfile
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, cast

from dlt._workspace.cli.utils import cli_host_command, mcp_stdio_args
from dlt._workspace.deployment.agent.exceptions import LocalToolError
from dlt._workspace.typing import TWorkspaceAccess, TWorkspaceLocalVerb

MAX_TOOL_OUTPUT = 20_000
SUBPROCESS_TIMEOUT = 120

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

SHELL_TOOL = "PowerShell" if os.name == "nt" else "Bash"
"""The shell of this platform, named as the Claude Code CLI names it. One shell per platform, so
the model is never told `bash` and handed PowerShell."""

LOCAL_TOOLS: Dict[str, Tuple[str, ...]] = {
    "read": ("Read", "Glob", "Grep"),
    "write": ("Write", "Edit"),
    "execute": (SHELL_TOOL, "RunPython"),
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


def tool_env() -> Dict[str, str]:
    """Child environment with the job's virtualenv on PATH, so `dlthub`, `dlt` and `python` resolve."""
    bin_dir = Path(sys.executable).parent
    env = dict(os.environ)
    if str(bin_dir) not in env.get("PATH", "").split(os.pathsep):
        env["PATH"] = os.pathsep.join(filter(None, [str(bin_dir), env.get("PATH", "")]))
    if (bin_dir.parent / "pyvenv.cfg").is_file():
        env["VIRTUAL_ENV"] = str(bin_dir.parent)
    # the MCP server we spawn writes to our stderr, so it keeps quiet
    env["FASTMCP_SHOW_SERVER_BANNER"] = "false"
    env["FASTMCP_LOG_LEVEL"] = "WARNING"
    # tool output is decoded as UTF-8, and a Windows console would otherwise write its code page
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    return env


def shell_executable() -> str:
    """The shell behind `SHELL_TOOL`: `pwsh` or `powershell` on Windows, `bash` elsewhere."""
    if os.name != "nt":
        return "bash"
    # `bash` on a Windows PATH is usually the WSL launcher, so it is never a fallback here
    return shutil.which("pwsh") or shutil.which("powershell") or "powershell"


def temp_dir() -> Path:
    """The system temp folder, where an agent may keep scratch files outside the workspace."""
    return Path(tempfile.gettempdir()).resolve()


def workspace_note(root: Any, scratch: Any) -> str:
    """One sentence for the system prompt naming the workspace and where scratch files go."""
    # every path the model sees is posix, whatever the platform
    root, scratch = Path(root).as_posix(), Path(scratch).as_posix()
    return f"The workspace is `{root}`. Scratch files belong in the temp folder `{scratch}`."


class LocalTools:
    """File, search and execution tools confined to the workspace root and a scratch folder.

    Each method is one tool as the model sees it, and its docstring is the tool description.
    Nothing here is a sandbox: the shell and `python` run in the job's own process tree and
    virtualenv, and the runner the job already runs in is what contains them.
    """

    def __init__(self, workspace_root: str, scratch_dir: Optional[str] = None) -> None:
        self.root = Path(workspace_root).resolve()
        self.scratch = Path(scratch_dir).resolve() if scratch_dir else temp_dir()

    def by_name(self) -> Dict[str, Callable[..., str]]:
        """The tools under the names in `LOCAL_TOOLS`."""
        return {
            "Read": self.read,
            "Glob": self.glob,
            "Grep": self.grep,
            "Write": self.write,
            "Edit": self.edit,
            SHELL_TOOL: self.powershell if os.name == "nt" else self.bash,
            "RunPython": self.python,
        }

    def resolve(self, path: str) -> Path:
        """Resolves `path` against the workspace, refusing what escapes it and the scratch folder."""
        # an absolute path replaces `root` in the join, which is how a scratch file is named
        target = (self.root / path).resolve()
        if not (target.is_relative_to(self.root) or target.is_relative_to(self.scratch)):
            raise LocalToolError(
                f"{path!r} is outside the workspace and the temp folder {str(self.scratch)!r}"
            )
        if is_secret_file(target.name):
            raise LocalToolError(f"{path!r} holds credentials and cannot be opened")
        return target

    def read(self, path: str, offset: int = None, limit: int = None) -> str:
        """Read a UTF-8 text file, whole or a range of lines.

        Args:
            path: Path relative to the workspace root, or an absolute path inside the temp folder.
            offset: First line to return, 1-based. Reads from the start when omitted.
            limit: How many lines to return. Reads to the end when omitted.
        """
        target = self.resolve(path)
        if not target.is_file():
            raise LocalToolError(f"{path!r} does not exist")
        text = target.read_text(encoding="utf-8", errors="replace")
        if offset is None and limit is None:
            return text[:MAX_TOOL_OUTPUT]
        start = max((offset or 1) - 1, 0)
        lines = text.splitlines(keepends=True)[start : None if limit is None else start + limit]
        return "".join(lines)[:MAX_TOOL_OUTPUT]

    def glob(self, pattern: str = "*") -> str:
        """List workspace files matching a glob, one relative path per line.

        Args:
            pattern: Glob relative to the workspace root, e.g. `"**/*.py"`.
        """
        matches = sorted(
            path.relative_to(self.root).as_posix()
            for path in self.root.glob(pattern)
            if path.is_file() and not is_secret_file(path.name)
        )
        return "\n".join(matches)[:MAX_TOOL_OUTPUT] or f"(nothing matches {pattern!r})"

    def grep(self, pattern: str, glob: str = "**/*") -> str:
        """Search workspace file contents, returning `path:line:text` for each hit.

        Args:
            pattern: Regular expression matched against each line.
            glob: Which files to search, relative to the workspace root.
        """
        try:
            expression = re.compile(pattern)
        except re.error as ex:
            raise LocalToolError(f"{pattern!r} is not a valid regular expression: {ex}") from ex
        hits: List[str] = []
        for path in sorted(self.root.glob(glob)):
            if not path.is_file() or is_secret_file(path.name):
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            relative = path.relative_to(self.root).as_posix()
            hits += [
                f"{relative}:{number}:{line}"
                for number, line in enumerate(text.splitlines(), start=1)
                if expression.search(line)
            ]
        return "\n".join(hits)[:MAX_TOOL_OUTPUT] or f"(no line matches {pattern!r})"

    def write(self, path: str, content: str) -> str:
        """Write a UTF-8 text file, creating parent folders.

        Args:
            path: Path relative to the workspace root, or an absolute path inside the temp folder.
            content: Full new contents of the file. Replaces what is there.
        """
        target = self.resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return f"wrote {len(content)} characters to {path}"

    def edit(self, path: str, old_text: str, new_text: str) -> str:
        """Replace one fragment of a file, leaving the rest untouched.

        Args:
            path: Path relative to the workspace root, or an absolute path inside the temp folder.
            old_text: Text to replace. Must appear exactly once in the file.
            new_text: Text to put in its place.
        """
        target = self.resolve(path)
        if not target.is_file():
            raise LocalToolError(f"{path!r} does not exist")
        text = target.read_text(encoding="utf-8", errors="replace")
        found = text.count(old_text)
        if found != 1:
            raise LocalToolError(
                f"{old_text[:80]!r} appears {found} times in {path!r}."
                " Include more context, so it appears exactly once"
            )
        target.write_text(text.replace(old_text, new_text), encoding="utf-8")
        return f"replaced {len(old_text)} characters in {path}"

    def bash(self, command: str) -> str:
        """Run a bash command and return its stdout and stderr.

        Every call starts a fresh shell in the workspace root. Nothing carries over between
        calls, so `cd` in one call does not affect the next: use paths relative to the
        workspace root, or change directory inside the same command (`cd subdir && ls`). The
        job's virtualenv is first on PATH, so `dlthub`, `dlt` and `python` are the workspace's own.

        Args:
            command: Command line, run through `bash -c`.
        """
        return self._capture([shell_executable(), "-c", command])

    def powershell(self, command: str) -> str:
        """Run a PowerShell command and return its stdout and stderr.

        Every call starts a fresh shell in the workspace root. Nothing carries over between
        calls, so `cd` in one call does not affect the next: use paths relative to the
        workspace root, or change directory inside the same command (`cd subdir; ls`). The
        job's virtualenv is first on PATH, so `dlthub`, `dlt` and `python` are the workspace's own.

        Args:
            command: PowerShell command line, run non-interactively without a profile.
        """
        # the console code page is not UTF-8 on Windows, and the output is decoded as UTF-8
        script = f"[Console]::OutputEncoding = [Text.UTF8Encoding]::new(); {command}"
        return self._capture(
            [
                shell_executable(),
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                script,
            ]
        )

    def python(self, code: str) -> str:
        """Run Python in the workspace environment and return its stdout and stderr.

        The interpreter is the workspace's own virtualenv, running in the workspace root: `dlt`,
        the workspace pipelines and every configured destination import exactly as they do in
        the job itself, under the same profile and credentials. Each call is a fresh process, so
        nothing carries over between calls and only what you print comes back.

        Args:
            code: Python source to execute.
        """
        return self._capture([sys.executable, "-"], stdin=code)

    def _capture(self, argv: List[str], stdin: str = None) -> str:
        """Runs a child process in the workspace and returns its combined, capped output."""
        try:
            done = subprocess.run(  # noqa: S603
                argv,
                input=stdin,
                cwd=str(self.root),
                env=tool_env(),
                capture_output=True,
                encoding="utf-8",
                errors="replace",
                timeout=SUBPROCESS_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            return f"(timed out after {SUBPROCESS_TIMEOUT}s)"
        except OSError as ex:
            raise LocalToolError(f"could not run {argv[0]!r}: {ex}") from ex
        output = (done.stdout + done.stderr).strip()
        return output[:MAX_TOOL_OUTPUT] or f"(no output, exit {done.returncode})"


def mcp_server_command(tools: List[str], access: TWorkspaceAccess) -> Dict[str, Any]:
    """Stdio server config serving the feature groups an agent declared, limited to its access."""

    return {
        "type": "stdio",
        "command": cli_host_command(),
        "args": mcp_stdio_args(tools, with_defaults=False, access=access or {}),
        "env": tool_env(),
    }
