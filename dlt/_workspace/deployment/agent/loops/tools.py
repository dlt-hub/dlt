"""Tools for agent loops: the local tools, the environment they run in and the workspace MCP server."""

import os
import re
import shutil
import subprocess
import sys
import tempfile
from fnmatch import fnmatchcase
from itertools import islice
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    cast,
)

from dlt._workspace.cli.utils import cli_host_command, mcp_stdio_args
from dlt._workspace.deployment.agent.exceptions import LocalToolError
from dlt._workspace.deployment.file_selector import GitignoreFileSelector
from dlt._workspace.typing import TWorkspaceAccess, TWorkspaceLocalVerb

MAX_TOOL_OUTPUT = 20_000
"""Maximum number of characters that the shell and Python tools return."""
SUBPROCESS_TIMEOUT = 120
"""Seconds after which the shell and Python tools stop a command."""
TOOL_LINE_LIMIT = 2000
"""Number of lines that `Read`, `Glob` and `Grep` return if the model gives no `limit`."""
TOOL_MAX_CHARS = 100_000
"""Maximum number of characters that `Read`, `Glob` and `Grep` return."""
BINARY_SNIFF_BYTES = 8192
"""Number of bytes at the start of a file that `Grep` checks for a NUL byte."""
LINE_ENDINGS = "\r\n"

MCP_SERVER_ID = "dlt-workspace-mcp"
"""Name of the workspace MCP server in the loops and in `dlthub ai mcp install`."""

SECRET_FILE_PATTERNS = ("*secrets.toml", ".env", ".env.*")
"""Credential files that the file tools do not open: `[<profile>.]secrets.toml` and dotenv files."""

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
"""Claude Code CLI tools that use file paths. Each tool gets a rule that blocks credential files."""

SHELL_TOOL = "PowerShell" if os.name == "nt" else "Bash"
"""Name of the shell tool on this platform, as the Claude Code CLI names it."""

LOCAL_TOOLS: Dict[str, Tuple[str, ...]] = {
    "read": ("Read", "Glob", "Grep"),
    "write": ("Write", "Edit"),
    "execute": (SHELL_TOOL, "RunPython"),
    "network": ("WebFetch", "WebSearch"),
}
"""Tools that each `access.local` verb gives, with the names that the Claude Code CLI uses."""

LOCAL_TOOL_VERBS: Dict[str, TWorkspaceLocalVerb] = {
    name: cast(TWorkspaceLocalVerb, verb) for verb, names in LOCAL_TOOLS.items() for name in names
}
"""The `access.local` verb of each local tool."""


def is_secret_file(path: str) -> bool:
    """True if `path` is a credential file, in any folder."""
    name = os.path.basename(path)
    return any(fnmatchcase(name, pattern) for pattern in SECRET_FILE_PATTERNS)


def take_page(
    items: Iterable[str], offset: Optional[int], limit: Optional[int]
) -> Tuple[List[str], int, bool]:
    """Takes one page of newline-terminated `items`, starting at the 1-based `offset`.

    Returns the page, the number of its first item, and True if more items follow.
    """
    first = max(offset or 1, 1)
    page: List[str] = []
    size = 0
    for item in islice(items, first - 1, None):
        # reads at most one item past the page, to know that more follow
        if len(page) == (limit or TOOL_LINE_LIMIT) or (page and size + len(item) > TOOL_MAX_CHARS):
            return page, first, True
        # full lines only; a single line longer than the limit is cut
        if len(item) > TOOL_MAX_CHARS:
            item = item[:TOOL_MAX_CHARS] + "…\n"
        page.append(item)
        size += len(item)
    return page, first, False


def page_note(unit: str, first: int, count: int, total: Optional[int] = None) -> str:
    """The last line of a page that is not the end. It gives the `offset` of the next page."""
    last = first + count - 1
    of_total = f" of {total}" if total is not None else ""
    return f"({unit} {first}-{last}{of_total} shown; more with offset={last + 1})"


def _is_binary(path: Path) -> bool:
    """True if the start of the file has a NUL byte, or if the file cannot be read."""
    try:
        with path.open("rb") as file:
            return b"\0" in file.read(BINARY_SNIFF_BYTES)
    except OSError:
        return True


def secret_deny_rules() -> List[str]:
    """Claude Code CLI rules that block its file tools from credential files."""
    return [f"{tool}(**/{pattern})" for tool in FILE_TOOLS for pattern in SECRET_FILE_PATTERNS]


def tool_env() -> Dict[str, str]:
    """Environment for the processes that the tools start."""
    bin_dir = Path(sys.executable).parent
    env = dict(os.environ)
    # the job venv goes first on `PATH`, so `python`, `dlt` and `dlthub` come from it
    if str(bin_dir) not in env.get("PATH", "").split(os.pathsep):
        env["PATH"] = os.pathsep.join(filter(None, [str(bin_dir), env.get("PATH", "")]))
    if (bin_dir.parent / "pyvenv.cfg").is_file():
        env["VIRTUAL_ENV"] = str(bin_dir.parent)
    # the MCP server writes to the stderr of this process, so keep it quiet
    env["FASTMCP_SHOW_SERVER_BANNER"] = "false"
    env["FASTMCP_LOG_LEVEL"] = "WARNING"
    # the tools read output as UTF-8. Without this, a Windows console writes in its own code page
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    return env


def shell_executable() -> str:
    """The program behind `SHELL_TOOL`: `bash`, or `pwsh` or `powershell` on Windows."""
    if os.name != "nt":
        return "bash"
    # on Windows, `bash` on the PATH is usually the WSL launcher, so it is never used here
    return shutil.which("pwsh") or shutil.which("powershell") or "powershell"


def temp_dir() -> Path:
    """The temp folder of the system. An agent keeps its scratch files there."""
    return Path(tempfile.gettempdir()).resolve()


def workspace_note(root: Any, scratch: Any) -> str:
    """A sentence for the system prompt. It gives the workspace folder and the temp folder."""
    # the model sees all paths with forward slashes, on all platforms
    root, scratch = Path(root).as_posix(), Path(scratch).as_posix()
    return f"The workspace is `{root}`. Scratch files belong in the temp folder `{scratch}`."


class LocalTools:
    """File, search and run tools of an agent, limited to the workspace and the temp folder.

    Note: the docstring of each tool method is the tool description that the model reads.
    Warning: not a sandbox. Commands run in the process tree and virtual environment of the job.
    """

    def __init__(self, workspace_root: str, scratch_dir: Optional[str] = None) -> None:
        self.root = Path(workspace_root).resolve()
        self.scratch = Path(scratch_dir).resolve() if scratch_dir else temp_dir()

    def by_name(self) -> Dict[str, Callable[..., str]]:
        """The tools, with the names from `LOCAL_TOOLS`."""
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
        """The absolute form of `path`. Refuses credential files and paths outside the workspace
        and the temp folder."""
        # an absolute path replaces `root` in the join. That is how a scratch file is named
        target = self._confine(self.root / path, path)
        # a link named like a credential file is refused too, wherever it points
        if is_secret_file(path) or is_secret_file(target.name):
            raise LocalToolError(f"{path!r} holds credentials and cannot be opened")
        return target

    def _confine(self, path: Path, shown: Any) -> Path:
        """`path` with symlinks followed, refused outside the workspace and the temp folder."""
        # resolved before the check, so a link cannot point past either folder
        target = path.resolve()
        if not (target.is_relative_to(self.root) or target.is_relative_to(self.scratch)):
            raise LocalToolError(
                f"{shown!r} is outside the workspace and the temp folder {str(self.scratch)!r}"
            )
        return target

    def _reachable(self, path: Path) -> Optional[Path]:
        """`path` with symlinks followed, or `None` outside the workspace and the temp folder."""
        try:
            return self._confine(path, path)
        except LocalToolError:
            return None

    def read(
        self, path: str, offset: int = None, limit: int = None, line_numbers: bool = True
    ) -> str:
        """Reads a text file.

        - Reads up to 2000 lines by default.
        - Each line starts with its line number and a tab. Line numbers start at 1.
        - If the file has more lines, the last line gives the `offset` that reads on.

        Args:
            path: Relative to the workspace, or absolute in the temp folder.
            offset: First line to read.
            limit: Number of lines to read.
            line_numbers: Prefix each line with its number.
        """
        target = self.resolve(path)
        if not target.is_file():
            raise LocalToolError(f"{path!r} does not exist")
        lines = target.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
        if not lines:
            return f"({path} is empty)"
        if (offset or 1) > len(lines):
            return f"({path} has {len(lines)} lines, offset {offset} is past its end)"
        numbered = (f"{n}\t{line}" if line_numbers else line for n, line in enumerate(lines, 1))
        page, first, more = take_page(numbered, offset, limit)
        return "".join(page) + (page_note("lines", first, len(page), len(lines)) if more else "")

    def glob(
        self,
        pattern: str = "**/*",
        include_ignored: bool = False,
        offset: int = None,
        limit: int = None,
    ) -> str:
        """Finds files by name. Returns paths relative to the workspace, one per line.

        - Patterns use `.gitignore` syntax: `*.py` matches in all folders, `jobs/*.py` only in
          `jobs`.
        - Skips the `.git` folder and the files that `.gitignore` excludes.
        - Returns up to 2000 paths. If more match, the last line gives the `offset` that reads on.

        Args:
            pattern: Pattern of the files to find.
            include_ignored: Also find the files that `.gitignore` excludes.
            offset: First path to return.
            limit: Number of paths to return.
        """
        paths = (
            f"{relative.as_posix()}\n"
            for path, relative in self._select(pattern, include_ignored)
            if self._reachable(path)
        )
        page, first, more = take_page(paths, offset, limit)
        if not page:
            return f"(nothing matches {pattern!r})"
        return "".join(page) + (page_note("paths", first, len(page)) if more else "")

    def grep(
        self,
        pattern: str,
        glob: str = "**/*",
        include_ignored: bool = False,
        offset: int = None,
        limit: int = None,
    ) -> str:
        """Searches file contents. Returns `path:line_number:text` for each matching line.

        - `pattern` is a Python regular expression, matched within single lines.
        - `glob` uses the same syntax as the Glob tool.
        - Skips credential files, binary files, the `.git` folder and the files that
          `.gitignore` excludes.
        - Returns up to 2000 matches. If there are more, the last line gives the `offset` that
          reads on.

        Args:
            pattern: Regular expression to find.
            glob: Pattern of the files to search.
            include_ignored: Also search the files that `.gitignore` excludes.
            offset: First match to return.
            limit: Number of matches to return.
        """
        try:
            expression = re.compile(pattern)
        except re.error as ex:
            raise LocalToolError(f"{pattern!r} is not a valid regular expression: {ex}") from ex
        hits = self._matching_lines(expression, glob, include_ignored)
        try:
            page, first, more = take_page(hits, offset, limit)
        finally:
            # stop the search at the end of the page and close the open file
            hits.close()
        if not page:
            return f"(no line matches {pattern!r})"
        return "".join(page) + (page_note("matches", first, len(page)) if more else "")

    def _select(self, pattern: str, include_ignored: bool) -> GitignoreFileSelector:
        """Workspace files that `pattern` matches."""
        try:
            return GitignoreFileSelector(
                str(self.root), include=[pattern], use_ignore_file=not include_ignored
            )
        except ImportError as ex:
            raise LocalToolError(
                "Glob and Grep need the `pathspec` package, which is not installed. Use Read, or"
                " list and search files with the shell"
            ) from ex

    def _matching_lines(
        self, expression: "re.Pattern[str]", glob: str, include_ignored: bool
    ) -> Generator[str, None, None]:
        """One `path:line_number:text` line for each match. Reads the files only as far as the
        caller takes lines."""
        for path, relative_path in self._select(glob, include_ignored):
            target = self._reachable(path)
            if target is None or is_secret_file(path.name) or is_secret_file(target.name):
                continue
            if _is_binary(target):
                continue
            relative = relative_path.as_posix()
            try:
                with target.open(encoding="utf-8", errors="replace") as file:
                    for number, line in enumerate(file, start=1):
                        if expression.search(line):
                            yield f"{relative}:{number}:{line.rstrip(LINE_ENDINGS)}\n"
            except OSError:
                continue

    def write(self, path: str, content: str) -> str:
        """Writes a text file. Replaces the file if it exists and creates missing folders.

        Args:
            path: Relative to the workspace, or absolute in the temp folder.
            content: Full text of the file.
        """
        target = self.resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return f"wrote {len(content)} characters to {path}"

    def edit(self, path: str, old_text: str, new_text: str) -> str:
        """Replaces text in a file.

        - `old_text` must occur exactly once. If it occurs more often, add the text around it.

        Args:
            path: Relative to the workspace, or absolute in the temp folder.
            old_text: Text to replace.
            new_text: Replacement text.
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
        """Runs a bash command in the workspace folder.

        - Each call starts a new shell. `cd` and `export` do not carry over to the next call.
        - `python`, `dlt` and `dlthub` come from the Python environment of the workspace.
        - Stops the command after 120 seconds. Returns the first 20000 characters of output.

        Args:
            command: Command to run.
        """
        return self._capture([shell_executable(), "-c", command])

    def powershell(self, command: str) -> str:
        """Runs a PowerShell command in the workspace folder.

        - Each call starts a new shell. `cd` and variables do not carry over to the next call.
        - `python`, `dlt` and `dlthub` come from the Python environment of the workspace.
        - Stops the command after 120 seconds. Returns the first 20000 characters of output.

        Args:
            command: Command to run.
        """
        # the Windows console does not use UTF-8, and the tools read output as UTF-8
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
        """Runs Python code in the workspace folder.

        - Each call starts a new process. Variables and imports do not carry over to the next call.
        - Uses the Python environment, configuration and credentials of the workspace, as the job
          does.
        - Stops the code after 120 seconds. Returns the first 20000 characters of output.

        Args:
            code: Code to run.
        """
        return self._capture([sys.executable, "-"], stdin=code)

    def _capture(self, argv: List[str], stdin: str = None) -> str:
        """Runs a process in the workspace folder. Returns its output and error output, cut to
        `MAX_TOOL_OUTPUT` characters."""
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
    """Command that starts the workspace MCP server over stdio. The server serves the feature
    groups that the agent declares, limited to its access."""

    return {
        "type": "stdio",
        "command": cli_host_command(),
        "args": mcp_stdio_args(tools, with_defaults=False, access=access or {}),
        "env": tool_env(),
    }
