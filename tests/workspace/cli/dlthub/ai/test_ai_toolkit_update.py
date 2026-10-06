import functools
import json
import os
import shutil
from pathlib import Path
from typing import Any, Iterator, Optional
from unittest.mock import patch

import pytest
import yaml

from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.cli.dlthub.ai.commands import (
    ai_toolkit_install_command,
    ai_toolkit_update_command,
)
from dlt._workspace.cli.dlthub.ai.agents import InstallAction
from dlt._workspace.cli.dlthub.ai.update import (
    FileCandidate,
    FileDecision,
    FileObservation,
    FileUpdate,
    classify_file,
    failed_dependencies,
    present_components,
    toolkit_update_order,
)
from dlt._workspace.cli.dlthub.ai.utils import compute_file_hash, load_toolkits_index

from tests.workspace.cli.dlthub.ai.utils import make_mock_toolkit, make_versioned_workbench


@pytest.fixture
def project_root() -> Iterator[Path]:
    """Empty project set as the active run context."""
    root = Path("project")
    root.mkdir()
    with patch("dlt.common.runtime.run_context.active") as mock_ctx:
        settings_dir = str(root / ".dlt")
        mock_ctx.return_value.run_dir = str(root)
        mock_ctx.return_value.settings_dir = settings_dir
        mock_ctx.return_value.get_setting = functools.partial(os.path.join, settings_dir)
        yield root


def _patch_workbench(base: Path) -> Any:
    return patch("dlt._workspace.cli.dlthub.ai.commands.fetch_workbench_base", return_value=base)


def _set_plugin_field(toolkit_dir: Path, key: str, value: Any) -> None:
    """Sets `key` in the toolkit's plugin.json, removes it when `value` is `None`."""
    plugin_json = toolkit_dir / ".claude-plugin" / "plugin.json"
    meta = json.loads(plugin_json.read_text(encoding="utf-8"))
    if value is None:
        meta.pop(key, None)
    else:
        meta[key] = value
    plugin_json.write_text(json.dumps(meta), encoding="utf-8")


def _bump_rule(rule_md: Path) -> None:
    """Changes a workbench rule so the next update has something to write."""
    rule_md.write_text("---\ndescription: Rule\n---\n# v2\n", encoding="utf-8")


ABSENT = FileObservation(exists=False, matches=False, disk_hash=None)
MATCHING = FileObservation(exists=True, matches=True, disk_hash="recorded")
CLEAN = FileObservation(exists=True, matches=False, disk_hash="recorded")
EDITED = FileObservation(exists=True, matches=False, disk_hash="edited")


@pytest.mark.parametrize(
    ("observed", "recorded", "force", "expected"),
    [
        (ABSENT, None, False, FileDecision("new", True, "disk")),
        (MATCHING, "recorded", False, FileDecision("unchanged", False, "disk")),
        (MATCHING, None, False, FileDecision("unchanged", False, "disk")),
        (CLEAN, "recorded", False, FileDecision("updated", True, "disk")),
        (EDITED, "recorded", False, FileDecision("modified", False, "recorded")),
        (EDITED, "recorded", True, FileDecision("modified", True, "disk")),
        (ABSENT, "recorded", False, FileDecision("deleted", False, "recorded")),
        (ABSENT, "recorded", True, FileDecision("deleted", True, "disk")),
        (EDITED, None, False, FileDecision("untracked", False, "none")),
        (EDITED, None, True, FileDecision("untracked", True, "disk")),
    ],
    ids=[
        "new",
        "unchanged-tracked",
        "unchanged-untracked-adopted",
        "clean-updated",
        "modified-skipped",
        "modified-forced",
        "deleted-skipped",
        "deleted-forced",
        "untracked-skipped",
        "untracked-forced",
    ],
)
def test_classify_file(
    observed: FileObservation,
    recorded: Optional[str],
    force: bool,
    expected: FileDecision,
) -> None:
    assert classify_file(observed, recorded, force) == expected


def test_present_components() -> None:
    """A component is present when any of its files is written or unchanged, skipped
    files do not count, and each component is listed once."""
    skill = InstallAction("skill", "s", Path("p/skills/s"), "copytree", Path("src/s"), False)
    rule = InstallAction("rule", "r", Path("p/rules/r.md"), "save", "# r", False)
    deleted = InstallAction("rule", "d", Path("p/rules/d.md"), "save", "# d", False)

    def _update(source: InstallAction, rel_path: str, decision: FileDecision) -> FileUpdate:
        return FileUpdate(FileCandidate(rel_path, Path(rel_path), "", source), decision)

    updates = [
        _update(skill, "skills/s/SKILL.md", FileDecision("unchanged", False, "disk")),
        _update(skill, "skills/s/helper.py", FileDecision("modified", False, "recorded")),
        _update(rule, "rules/r.md", FileDecision("updated", True, "disk")),
        _update(deleted, "rules/d.md", FileDecision("deleted", False, "recorded")),
    ]
    assert present_components(updates) == [skill, rule]


def test_toolkit_update_flow(project_root: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Update keeps local edits and removed files, adds new files, never replaces MCP config,
    `--force` overwrites, and a no-op update leaves the index untouched."""
    toolkit_dir = make_mock_toolkit(with_mcp=True)
    base = toolkit_dir.parent
    skill_dir = project_root / ".claude" / "skills" / "find-source"
    rule_path = project_root / ".claude" / "rules" / "test-toolkit-coding.md"
    command_path = project_root / ".claude" / "commands" / "bootstrap.md"
    mcp_path = project_root / ".mcp.json"

    with _patch_workbench(base):
        ai_toolkit_install_command(name="test-toolkit", agent="claude", location="mock://repo")
    installed_files = load_toolkits_index()["test-toolkit"]["files"]

    # upstream: new version changes a rule and a skill file, adds a rule, drops a command
    _set_plugin_field(toolkit_dir, "version", "0.2.0")
    (toolkit_dir / "rules" / "coding.md").write_text(
        "---\ndescription: Coding rule\n---\n# Coding v2\n", encoding="utf-8"
    )
    (toolkit_dir / "rules" / "testing.md").write_text(
        "---\ndescription: Testing rule\n---\n# Testing\n", encoding="utf-8"
    )
    (toolkit_dir / "skills" / "find-source" / "helper.py").write_text(
        "# helper v2\n", encoding="utf-8"
    )
    (toolkit_dir / "commands" / "bootstrap.md").unlink()

    # local: user edits a skill file and customizes the MCP server
    (skill_dir / "helper.py").write_text("# my helper\n", encoding="utf-8")
    mcp_config = json.loads(mcp_path.read_text(encoding="utf-8"))
    mcp_config["mcpServers"]["dlt-workspace-mcp"]["args"] = ["custom"]
    mcp_path.write_text(json.dumps(mcp_config), encoding="utf-8")
    custom_mcp = mcp_path.read_text(encoding="utf-8")
    capsys.readouterr()

    with _patch_workbench(base):
        ai_toolkit_update_command(name=None, location="mock://repo")
    out = capsys.readouterr().out
    entry = load_toolkits_index()["test-toolkit"]

    assert "Updating toolkit test-toolkit 0.1.0 -> 0.2.0" in out
    assert "# Coding v2" in rule_path.read_text(encoding="utf-8")
    assert (project_root / ".claude" / "rules" / "test-toolkit-testing.md").is_file()
    # local edit kept, its old hash stays recorded so the edit remains detectable
    assert (skill_dir / "helper.py").read_text(encoding="utf-8") == "# my helper\n"
    helper_rel = ".claude/skills/find-source/helper.py"
    assert "Skipping %s: modified locally" % helper_rel in out
    assert entry["files"][helper_rel] == installed_files[helper_rel]
    # file removed upstream is left in place and stays tracked
    assert command_path.is_file()
    assert ".claude/commands/bootstrap.md is no longer part of the toolkit" in out
    assert ".claude/commands/bootstrap.md" in entry["files"]
    # customized MCP server is not replaced
    assert mcp_path.read_text(encoding="utf-8") == custom_mcp
    assert entry["mcp_servers"] == ["dlt-workspace-mcp"]
    assert entry["version"] == "0.2.0"
    assert "Use --force to overwrite skipped files." in out

    # force overwrites the local edit and records the new hash
    with _patch_workbench(base):
        ai_toolkit_update_command(name="test-toolkit", location="mock://repo", force=True)
    out = capsys.readouterr().out
    assert "~ %s (forced)" % helper_rel in out
    assert (skill_dir / "helper.py").read_text(encoding="utf-8") == "# helper v2\n"
    entry = load_toolkits_index()["test-toolkit"]
    assert entry["files"][helper_rel]["sha3_256"] == compute_file_hash(skill_dir / "helper.py")
    assert mcp_path.read_text(encoding="utf-8") == custom_mcp

    # nothing left to do: index is not rewritten
    with _patch_workbench(base):
        ai_toolkit_update_command(name="test-toolkit", location="mock://repo")
    out = capsys.readouterr().out
    assert "Toolkit test-toolkit is up to date." in out
    assert load_toolkits_index()["test-toolkit"]["installed_at"] == entry["installed_at"]


def test_toolkit_update_codex_skill_and_agents_md(
    project_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Codex: the capped SKILL.md is compared and protected as the installed file, and
    AGENTS.md registrations are restored for rule skills already on disk."""
    toolkit_dir = make_mock_toolkit()
    base = toolkit_dir.parent
    (toolkit_dir / "skills" / "find-source" / "SKILL.md").write_text(
        "---\nname: find-source\ndescription: %s\n---\nFind a source.\n" % ("x" * 2000),
        encoding="utf-8",
    )
    skill_md = project_root / ".agents" / "skills" / "find-source" / "SKILL.md"
    agents_md = project_root / "AGENTS.md"

    with _patch_workbench(base):
        ai_toolkit_install_command(name="test-toolkit", agent="codex", location="mock://repo")
    assert "test-toolkit-coding" in agents_md.read_text(encoding="utf-8")
    capsys.readouterr()

    # untouched install is up to date: the capped SKILL.md matches what update renders
    with _patch_workbench(base):
        ai_toolkit_update_command(name="test-toolkit", location="mock://repo")
    assert "Toolkit test-toolkit is up to date." in capsys.readouterr().out

    # a local edit of SKILL.md is not overwritten by the follow-up capping save
    skill_md.write_text("---\nname: find-source\n---\nMine.\n", encoding="utf-8")
    agents_md.unlink()
    with _patch_workbench(base):
        ai_toolkit_update_command(name="test-toolkit", location="mock://repo")
    out = capsys.readouterr().out
    assert "Skipping .agents/skills/find-source/SKILL.md: modified locally" in out
    assert skill_md.read_text(encoding="utf-8") == "---\nname: find-source\n---\nMine.\n"
    # rule skill was unchanged, yet its registration is written again
    assert "test-toolkit-coding" in agents_md.read_text(encoding="utf-8")


def test_toolkit_update_all_in_dependency_order(
    project_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Without a name every installed toolkit is updated, dependencies first."""
    base = make_versioned_workbench(version="1.0.0")
    with _patch_workbench(base):
        ai_toolkit_install_command(name="my-toolkit", agent="claude", location="mock://repo")
    (base / "init" / "rules" / "base.md").write_text(
        "---\ndescription: Base rules\n---\n# Base v2\n", encoding="utf-8"
    )
    capsys.readouterr()

    with _patch_workbench(base):
        ai_toolkit_update_command(name=None, location="mock://repo")
    out = capsys.readouterr().out

    assert out.index("Updating toolkit init") < out.index("Updating toolkit my-toolkit")
    assert "# Base v2" in (project_root / ".claude" / "rules" / "init-base.md").read_text(
        encoding="utf-8"
    )
    assert "Toolkit my-toolkit is up to date." in out

    # metadata-only change upstream: a dropped dependency is recorded
    _set_plugin_field(base / "my-toolkit", "dependencies", None)
    with _patch_workbench(base):
        ai_toolkit_update_command(name="my-toolkit", location="mock://repo")
    assert "dependencies" not in load_toolkits_index()["my-toolkit"]


@pytest.mark.parametrize(
    ("breakage", "message"),
    [
        ("unknown-agent", "installed for unknown agent"),
        ("missing-transitive-dependency", "depends on"),
        ("dependency-cycle", "Circular dependency"),
    ],
    ids=["unknown-agent", "missing-transitive-dependency", "dependency-cycle"],
)
def test_toolkit_update_fails_before_writing(
    project_root: Path, capsys: pytest.CaptureFixture[str], breakage: str, message: str
) -> None:
    """A toolkit failing a safety gate is left unchanged and the command fails."""
    base = make_versioned_workbench(version="1.0.0")
    with _patch_workbench(base):
        ai_toolkit_install_command(name="my-toolkit", agent="claude", location="mock://repo")
    rule_path = project_root / ".claude" / "rules" / "my-toolkit-coding.md"
    installed_rule = rule_path.read_text(encoding="utf-8")
    (base / "my-toolkit" / "rules" / "coding.md").write_text(
        "---\ndescription: Coding rule\n---\n# Coding v2\n", encoding="utf-8"
    )

    if breakage == "unknown-agent":
        index_path = project_root / ".dlt" / ".toolkits"
        index = yaml.safe_load(index_path.read_text(encoding="utf-8"))
        index["my-toolkit"]["agent"] = "unknown"
        index_path.write_text(yaml.dump(index), encoding="utf-8")
    elif breakage == "missing-transitive-dependency":
        # my-toolkit -> init -> not-in-workbench
        _set_plugin_field(base / "init", "dependencies", ["not-in-workbench"])
    else:
        _set_plugin_field(base / "init", "dependencies", ["my-toolkit"])
    capsys.readouterr()

    with _patch_workbench(base), pytest.raises(CliCommandException):
        ai_toolkit_update_command(name="my-toolkit", location="mock://repo")

    assert message in capsys.readouterr().out
    assert rule_path.read_text(encoding="utf-8") == installed_rule


def test_toolkit_update_order_and_failed_dependencies() -> None:
    """Dependencies come first; a cycle neither raises nor blocks the toolkits in it."""
    dep_map = {"a": ["b"], "b": ["c"], "c": [], "loop": ["loop"]}
    assert toolkit_update_order({"a", "b", "c", "loop"}, dep_map) == ["c", "b", "a", "loop"]
    # not installed dependencies are not updated
    assert toolkit_update_order({"a"}, dep_map) == ["a"]
    assert failed_dependencies("a", dep_map, ["c"]) == ["c"]
    assert failed_dependencies("a", dep_map, ["loop"]) == []
    assert failed_dependencies("loop", dep_map, ["loop"]) == []


@pytest.mark.parametrize(
    ("breakage", "message"),
    [
        ("removed", "no longer in the workbench"),
        ("dependency-cycle", "Circular dependency"),
    ],
    ids=["removed", "dependency-cycle"],
)
def test_toolkit_update_all_continues_past_failed_toolkit(
    project_root: Path, capsys: pytest.CaptureFixture[str], breakage: str, message: str
) -> None:
    """A broken toolkit fails on its own: the others are updated, the command still fails."""
    base = make_versioned_workbench(version="1.0.0")
    # sorts before the healthy toolkits, so it fails first
    broken_dir = base / "aaa-broken"
    shutil.copytree(base / "init", broken_dir)
    _set_plugin_field(broken_dir, "name", "aaa-broken")
    with _patch_workbench(base):
        ai_toolkit_install_command(name="my-toolkit", agent="claude", location="mock://repo")
        ai_toolkit_install_command(name="aaa-broken", agent="claude", location="mock://repo")
    _bump_rule(base / "my-toolkit" / "rules" / "coding.md")
    if breakage == "removed":
        shutil.rmtree(broken_dir)
    else:
        _set_plugin_field(broken_dir, "dependencies", ["aaa-broken"])
    capsys.readouterr()

    with _patch_workbench(base), pytest.raises(CliCommandException):
        ai_toolkit_update_command(name=None, location="mock://repo")

    out = capsys.readouterr().out
    assert message in out
    assert "Could not update aaa-broken." in out
    rule_path = project_root / ".claude" / "rules" / "my-toolkit-coding.md"
    assert "# v2" in rule_path.read_text(encoding="utf-8")


def test_toolkit_update_all_skips_dependents_of_failed_toolkit(
    project_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A toolkit is not updated on top of a dependency that failed to update."""
    base = make_versioned_workbench(version="1.0.0")
    with _patch_workbench(base):
        ai_toolkit_install_command(name="my-toolkit", agent="claude", location="mock://repo")
    rule_path = project_root / ".claude" / "rules" / "my-toolkit-coding.md"
    installed_rule = rule_path.read_text(encoding="utf-8")
    _bump_rule(base / "my-toolkit" / "rules" / "coding.md")
    shutil.rmtree(base / "init")
    capsys.readouterr()

    with _patch_workbench(base), pytest.raises(CliCommandException):
        ai_toolkit_update_command(name=None, location="mock://repo")

    out = capsys.readouterr().out
    assert "Skipping toolkit my-toolkit: its dependency init failed to update." in out
    assert "Could not update init, my-toolkit." in out
    assert rule_path.read_text(encoding="utf-8") == installed_rule
