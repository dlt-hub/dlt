"""Tests for the local tools an agent loop serves: files, search and execution in a workspace."""

import os
import sys
from pathlib import Path

import pytest

from dlt._workspace.deployment.agent.exceptions import LocalToolError
from dlt._workspace.deployment.agent.loops.tools import (
    LOCAL_TOOLS,
    SHELL_TOOL,
    LocalTools,
    tool_env,
    workspace_note,
)


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

    assert tools.read("jobs/load.py") == "import dlt\nrows = 10\nprint(rows)\n"
    assert tools.read("jobs/load.py", offset=2, limit=1) == "rows = 10\n"
    assert tools.glob("**/*.py") == "jobs/load.py"
    assert tools.grep("rows") == "jobs/load.py:2:rows = 10\njobs/load.py:3:print(rows)"
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
    assert tools.read(str(scratch / "notes" / "draft.md")) == "one\nthree\n"
    # workspace paths stay relative to its root, absolute or not
    tools.write("out/report.md", "ok")
    assert tools.read(str(root / "out" / "report.md")) == "ok"
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
    # neither listing nor search offers it, and the rest of the folder still is
    assert "secrets.toml" not in tools.glob("**/*")
    assert tools.grep("api_key").startswith("(no line matches")
    assert "config.toml" in tools.glob("**/*")
    assert tools.read(".dlt/config.toml") == "[runtime]\n"


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
    # the model reads the docstrings, which say how execution behaves
    assert "fresh shell" in shell.__doc__ and "does not affect the next" in shell.__doc__
    assert "workspace environment" in tools.python.__doc__
