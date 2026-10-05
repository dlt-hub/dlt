"""Tests for the local tools an agent loop serves: files, search and execution in a workspace."""

import os
import sys
from pathlib import Path
from typing import Any

import pytest

from dlt._workspace.deployment.agent.exceptions import LocalToolError
from dlt._workspace.deployment.agent.loops.tools import (
    LOCAL_TOOLS,
    MAX_TOOL_OUTPUT,
    SHELL_TOOL,
    SUBPROCESS_TIMEOUT,
    TOOL_MAX_CHARS,
    LocalTools,
    tool_env,
    workspace_note,
)

from tests.workspace.utils import isolated_workspace


def test_every_local_verb_but_network_has_its_tools(tmp_path: Path) -> None:
    """`network` is the provider's; every other name in `LOCAL_TOOLS` is served here."""
    served = LocalTools(str(tmp_path)).by_name()
    for verb, names in LOCAL_TOOLS.items():
        if verb != "network":
            assert set(names) <= set(served), verb
    assert "WebSearch" not in served


def test_read_serves_whole_files_fragments_and_search(tmp_path: Path) -> None:
    """One verb, three ways to look: the file, a range of its lines, and a pattern."""
    (tmp_path / "jobs").mkdir()
    (tmp_path / "jobs" / "load.py").write_text("import dlt\nrows = 10\nprint(rows)\n")
    tools = LocalTools(str(tmp_path))

    assert tools.read("jobs/load.py") == "1\timport dlt\n2\trows = 10\n3\tprint(rows)\n"
    assert tools.read("jobs/load.py", line_numbers=False) == "import dlt\nrows = 10\nprint(rows)\n"
    # a range keeps the file's own line numbers and says where the rest starts
    assert (
        tools.read("jobs/load.py", offset=2, limit=1)
        == "2\trows = 10\n(lines 2-2 of 3 shown; more with offset=3)"
    )
    assert tools.glob("**/*.py") == "jobs/load.py\n"
    assert tools.grep("rows") == "jobs/load.py:2:rows = 10\njobs/load.py:3:print(rows)\n"
    assert "no line matches" in tools.grep("nothing-here")
    with pytest.raises(LocalToolError, match="does not exist"):
        tools.read("jobs/missing.py")
    with pytest.raises(LocalToolError, match="not a valid regular expression"):
        tools.grep("(")


def test_write_replaces_a_fragment_or_the_whole_file(tmp_path: Path) -> None:
    (tmp_path / "notes.md").write_text("one\ntwo\n")
    tools = LocalTools(str(tmp_path))

    tools.edit("notes.md", "two", "three")
    assert (tmp_path / "notes.md").read_text() == "one\nthree\n"
    # an ambiguous fragment is refused rather than guessed at
    (tmp_path / "notes.md").write_text("x\nx\n")
    with pytest.raises(LocalToolError, match="appears 2 times"):
        tools.edit("notes.md", "x", "y")

    tools.write("fresh/file.txt", "body")
    assert (tmp_path / "fresh" / "file.txt").read_text() == "body"


def test_file_tools_reach_the_workspace_and_the_scratch_folder(tmp_path: Path) -> None:
    """Scratch files go to the temp folder; nowhere else is reachable."""
    root, scratch = tmp_path / "ws", tmp_path / "scratch"
    root.mkdir()
    scratch.mkdir()
    tools = LocalTools(str(root), str(scratch))

    tools.write(str(scratch / "notes" / "draft.md"), "one\ntwo\n")
    tools.edit(str(scratch / "notes" / "draft.md"), "two", "three")
    assert tools.read(str(scratch / "notes" / "draft.md"), line_numbers=False) == "one\nthree\n"
    # workspace paths stay relative to its root, absolute or not
    tools.write("out/report.md", "ok")
    assert tools.read(str(root / "out" / "report.md"), line_numbers=False) == "ok"
    # anything else, including a hop out of either folder, is refused
    with pytest.raises(LocalToolError, match="outside the workspace and the temp folder"):
        tools.write(str(tmp_path / "elsewhere.txt"), "no")
    with pytest.raises(LocalToolError, match="outside the workspace and the temp folder"):
        tools.read("../elsewhere.txt")
    # credentials are off limits in the temp folder too
    with pytest.raises(LocalToolError, match="credentials"):
        tools.write(str(scratch / "secrets.toml"), "api_key = 'x'")
    # the prompt is where the model learns both folders
    note = workspace_note(tools.root, tools.scratch)
    assert root.resolve().as_posix() in note and scratch.resolve().as_posix() in note


def test_file_tools_refuse_credential_files(tmp_path: Path) -> None:
    """`local: [read]` is access to the workspace, not to what unlocks the destinations."""
    (tmp_path / ".dlt").mkdir()
    (tmp_path / ".dlt" / "prod.secrets.toml").write_text("[destination]\napi_key = 'live'\n")
    (tmp_path / ".dlt" / "config.toml").write_text("[runtime]\n")
    tools = LocalTools(str(tmp_path))

    with pytest.raises(LocalToolError, match="credentials"):
        tools.read(".dlt/prod.secrets.toml")
    with pytest.raises(LocalToolError, match="credentials"):
        tools.write(".dlt/secrets.toml", "api_key = 'stolen'")
    with pytest.raises(LocalToolError, match="credentials"):
        tools.edit(".dlt/prod.secrets.toml", "live", "stolen")
    # a listing names it, search never reads it
    assert ".dlt/prod.secrets.toml" in tools.glob("**/*")
    assert tools.grep("api_key").startswith("(no line matches")
    assert "config.toml" in tools.glob("**/*")
    assert tools.read(".dlt/config.toml") == "1\t[runtime]\n"


def test_read_pages_long_files_and_says_where_the_rest_starts(tmp_path: Path) -> None:
    (tmp_path / "long.txt").write_text("".join(f"line {n}\n" for n in range(1, 2501)))
    (tmp_path / "empty.txt").write_text("")
    (tmp_path / "wide.txt").write_text("x" * (TOOL_MAX_CHARS + 10) + "\nshort\n")
    tools = LocalTools(str(tmp_path))

    # without a limit a read stops at 2000 lines
    first = tools.read("long.txt").splitlines()
    assert first[0] == "1\tline 1" and first[-2] == "2000\tline 2000"
    assert first[-1] == "(lines 1-2000 of 2500 shown; more with offset=2001)"
    # the offset it names reads the rest, with nothing left to announce
    rest = tools.read("long.txt", offset=2001).splitlines()
    assert (rest[0], rest[-1], len(rest)) == ("2001\tline 2001", "2500\tline 2500", 500)
    assert (
        tools.read("long.txt", offset=2600)
        == "(long.txt has 2500 lines, offset 2600 is past its end)"
    )
    assert tools.read("empty.txt") == "(empty.txt is empty)"
    # a line longer than the character cap is cut, marked, and the next line waits for a new read
    wide = tools.read("wide.txt", line_numbers=False)
    assert wide.startswith("x" * 100) and "…\n" in wide
    assert wide.endswith("(lines 1-1 of 2 shown; more with offset=2)")


def test_glob_and_grep_skip_what_git_ignores() -> None:
    """A workspace with an ignored virtualenv, git internals, credentials and a binary file."""
    with isolated_workspace("default") as ctx:
        root = Path(ctx.run_dir)
        (root / "jobs").mkdir()
        (root / "jobs" / "load.py").write_text("def load_rows():\n    return 1\n")
        packages = root / ".venv" / "lib" / "site-packages" / "botocore"
        packages.mkdir(parents=True)
        for n in range(5):
            (packages / f"m{n}.py").write_text("def load_rows_handler():\n    pass\n")
        (root / ".git").mkdir()
        (root / ".git" / "config").write_text("load_rows\n")
        (root / ".gitignore").write_text(".venv/\n")
        with (root / ".dlt" / "secrets.toml").open("a") as secrets:
            secrets.write("\n# load_rows\n")
        (root / "image.bin").write_bytes(b"load_rows \0 binary")
        tools = LocalTools(ctx.run_dir)

        python_files = tools.glob("**/*.py").splitlines()
        assert "jobs/load.py" in python_files and "ducklake_pipeline.py" in python_files
        assert not any(path.startswith(".venv/") for path in python_files)
        # a pattern without a slash matches at any depth, as in .gitignore
        assert tools.glob("*.py").splitlines() == python_files
        # credentials, git internals, ignored and binary files are never searched
        assert tools.grep("load_rows") == "jobs/load.py:1:def load_rows():\n"
        # asked for, ignored files come back, git internals never do
        with_ignored = tools.glob("**/*", include_ignored=True)
        assert ".venv/lib/site-packages/botocore/m0.py" in with_ignored
        assert ".git/" not in with_ignored
        assert len(tools.grep("load_rows_handler", include_ignored=True).splitlines()) == 5
        # a credential file is listed by name
        assert ".dlt/secrets.toml" in tools.glob("**/*").splitlines()


def test_grep_pages_its_matches_and_stops_reading_at_the_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for n in range(3):
        (tmp_path / f"part{n}.txt").write_text("".join(f"hit {i}\n" for i in range(1500)))
    tools = LocalTools(str(tmp_path))

    page = tools.grep("hit").splitlines()
    assert page[0] == "part0.txt:1:hit 0" and page[-2] == "part1.txt:500:hit 499"
    assert page[-1] == "(matches 1-2000 shown; more with offset=2001)"
    rest = tools.grep("hit", offset=2001, limit=10).splitlines()
    assert rest[0] == "part1.txt:501:hit 500"
    assert rest[-1] == "(matches 2001-2010 shown; more with offset=2011)"
    assert tools.glob("*.txt", limit=2).splitlines()[-1] == "(paths 1-2 shown; more with offset=3)"

    # the search stops once the page is full: the third file is never opened
    opened = []
    open_file = Path.open

    def recording_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        opened.append(self.name)
        return open_file(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", recording_open)
    tools.grep("hit")
    assert "part2.txt" not in opened


def test_glob_and_grep_refuse_without_pathspec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "load.py").write_text("x = 1\n")
    monkeypatch.setitem(sys.modules, "pathspec", None)
    tools = LocalTools(str(tmp_path))

    with pytest.raises(LocalToolError, match="pathspec"):
        tools.glob("*.py")
    with pytest.raises(LocalToolError, match="pathspec"):
        tools.grep("x")
    assert tools.read("load.py") == "1\tx = 1\n"


def test_execution_runs_in_the_workspace_with_its_virtualenv(tmp_path: Path) -> None:
    """The launcher may start as `<venv>/bin/python -m ...`, which puts nothing on PATH."""
    env = tool_env()
    assert env["PATH"].split(os.pathsep)[0] == str(Path(sys.executable).parent)
    # the MCP server we spawn shares our stderr, so its banner and info logs would land there
    assert env["FASTMCP_SHOW_SERVER_BANNER"] == "false"
    assert env["FASTMCP_LOG_LEVEL"] == "WARNING"
    assert env["PYTHONIOENCODING"] == "utf-8"

    tools = LocalTools(str(tmp_path))
    (tmp_path / "marker.txt").write_text("here")
    # each call is a fresh process in the workspace root, on the workspace interpreter
    assert tools.python(
        "import os, sys; print(os.getcwd()); print(sys.executable)"
    ).splitlines() == [
        str(tmp_path.resolve()),
        sys.executable,
    ]
    # one shell per platform, PowerShell on Windows and bash elsewhere; `ls`, `cd`, `pwd` and
    # `echo` are aliases in PowerShell, so the same commands run on both
    shell = tools.by_name()[SHELL_TOOL]
    assert "marker.txt" in shell("ls")
    assert shell("cd /; pwd") != shell("pwd")
    assert shell("exit 3") == "(no output, exit 3)"
    # output is UTF-8 on every platform, whatever the console code page
    assert shell("echo zażółć") == "zażółć"
    assert tools.python("print('zażółć')") == "zażółć"
    # the model reads the docstrings, which say how execution behaves and give its real limits
    for tool in (shell, tools.python):
        description = " ".join(tool.__doc__.split())
        assert "Each call starts a new" in description
        assert "do not carry over to the next call" in description
        assert f"{SUBPROCESS_TIMEOUT} seconds" in description
        assert f"first {MAX_TOOL_OUTPUT} characters" in description
