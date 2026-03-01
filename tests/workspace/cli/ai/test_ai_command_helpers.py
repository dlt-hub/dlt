import functools
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, Set, Type
from unittest.mock import patch

import pytest
import tomlkit
import yaml

from dlt._workspace.cli.ai.commands import (
    _execute_install,
    _install_dependencies,
    _plan_toolkit_install,
    _report_and_execute,
    _resolve_agent,
)
from dlt._workspace.cli.ai.agents import (
    _AIAgent,
    _ClaudeAgent,
    _CodexAgent,
    _CursorAgent,
)
from dlt._workspace.cli.ai.utils import (
    build_toolkits_dependency_map,
    load_toolkits_index,
    resolve_toolkit_dependencies,
    scan_workbench_toolkits,
)
from dlt._workspace.cli.exceptions import CliCommandException

from tests.workspace.cli.ai.utils import (
    make_mock_toolkit,
    make_mock_toolkit_info,
    make_mock_workbench,
)


@pytest.mark.parametrize(
    ("variant_cls", "expected_kinds", "rule_path", "rule_check", "ignore_file"),
    [
        (
            _ClaudeAgent,
            {"skill", "command", "rule", "ignore"},
            ".claude/rules/test-toolkit-coding.md",
            lambda c: "alwaysApply" not in c and "# Coding Style" in c,
            ".claudeignore",
        ),
        (
            _CursorAgent,
            {"skill", "command", "rule", "ignore"},
            ".cursor/rules/test-toolkit-coding.mdc",
            lambda c: "alwaysApply: true" in c,
            ".cursorignore",
        ),
        (
            _CodexAgent,
            {"skill", "ignore"},
            ".agents/skills/test-toolkit-coding/SKILL.md",
            lambda c: "Coding Style" in c,
            ".codexignore",
        ),
    ],
    ids=["claude", "cursor", "codex"],
)
def test_toolkit_install_all_variants(
    variant_cls: Type[_AIAgent],
    expected_kinds: Set[str],
    rule_path: str,
    rule_check: Callable[..., bool],
    ignore_file: str,
) -> None:
    """Plans and executes a full install for each variant, verifying component types and output."""
    toolkit_dir = make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()

    variant = variant_cls()
    actions, _warnings = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")

    assert len(actions) == 4
    assert {a.kind for a in actions} == expected_kinds
    assert all(not a.conflict for a in actions)

    _execute_install(actions)

    # skill dir always copied with all files
    skill_base = variant.component_dir("skill", project_root) / "find-source"
    assert (skill_base / "SKILL.md").exists()
    assert (skill_base / "helper.py").exists()

    # rule/converted-rule written with correct content
    rule_dest = project_root / rule_path
    assert rule_dest.is_file()
    assert rule_check(rule_dest.read_text(encoding="utf-8"))

    # ignore file written with platform-specific name
    ignore_dest = project_root / ignore_file
    assert ignore_dest.is_file()
    assert "secrets.toml" in ignore_dest.read_text(encoding="utf-8")


def test_toolkit_install_skips_bad_frontmatter() -> None:
    """Skills and commands with invalid YAML frontmatter are skipped with a warning."""
    toolkit_dir = make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()

    # break the skill's SKILL.md frontmatter
    skill_md = toolkit_dir / "skills" / "find-source" / "SKILL.md"
    skill_md.write_text(
        "---\nname: find-source\nargument-hint: [pipeline] [-- <hints>]\n---\n# Body",
        encoding="utf-8",
    )
    # break a command's frontmatter
    cmd_md = toolkit_dir / "commands" / "bootstrap.md"
    cmd_md.write_text(
        "---\nname: Bootstrap\nhint: [bad: yaml[\n---\n# Bootstrap",
        encoding="utf-8",
    )

    variant = _ClaudeAgent()
    actions, warn_list = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")

    assert any("Skipping skill find-source" in w for w in warn_list)
    assert any("Skipping command bootstrap" in w for w in warn_list)

    assert any(a.kind == "rule" for a in actions)
    assert any(a.kind == "ignore" for a in actions)
    assert not any(a.source_name == "find-source" for a in actions)
    assert not any(a.source_name == "bootstrap" for a in actions)


def test_toolkit_install_strict_fails_on_bad_frontmatter(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """With --strict, validation warnings cause CliCommandException."""
    toolkit_dir = make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()

    skill_md = toolkit_dir / "skills" / "find-source" / "SKILL.md"
    skill_md.write_text(
        "---\nname: find-source\nargument-hint: [pipeline] [-- <hints>]\n---\n# Body",
        encoding="utf-8",
    )

    variant = _ClaudeAgent()
    actions, warn_list = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    assert len(warn_list) > 0

    with pytest.raises(CliCommandException):
        _report_and_execute(actions, warn_list, strict=True)


def test_toolkit_install_skip_existing() -> None:
    """Pre-existing destinations are flagged as conflicts and left untouched."""
    toolkit_dir = make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()

    variant = _ClaudeAgent()

    existing_skill = project_root / ".claude" / "skills" / "find-source"
    existing_skill.mkdir(parents=True)
    (existing_skill / "SKILL.md").write_text("custom content", encoding="utf-8")

    actions, _warnings = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")

    skill_action = next(a for a in actions if a.source_name == "find-source")
    assert skill_action.conflict is True

    installed = _execute_install(actions)
    assert installed == 3
    assert (existing_skill / "SKILL.md").read_text(encoding="utf-8") == "custom content"


@pytest.mark.parametrize(
    ("variant_cls", "config_rel_path", "top_key"),
    [
        (_ClaudeAgent, ".mcp.json", "mcpServers"),
        (_CursorAgent, ".cursor/mcp.json", "mcpServers"),
        (_CodexAgent, ".codex/config.toml", "mcp_servers"),
    ],
    ids=["claude", "cursor", "codex"],
)
def test_toolkit_install_mcp_all_variants(
    variant_cls: Type[_AIAgent],
    config_rel_path: str,
    top_key: str,
) -> None:
    """Plans and executes MCP install for each variant, verifying format."""
    toolkit_dir = make_mock_toolkit(with_mcp=True)
    project_root = Path("project")
    project_root.mkdir()

    variant = variant_cls()
    actions, _warnings = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")

    mcp_actions = [a for a in actions if a.kind == "mcp"]
    assert len(mcp_actions) == 1
    assert not mcp_actions[0].conflict
    assert isinstance(mcp_actions[0].content_or_path, str)

    _execute_install(actions)

    config_file = project_root / config_rel_path
    assert config_file.is_file()
    content = config_file.read_text(encoding="utf-8")

    if variant_cls in (_ClaudeAgent, _CursorAgent):
        data = json.loads(content)
        srv = data[top_key]["dlt-workspace-mcp"]
        assert srv["command"] == "uv"
        if variant_cls == _ClaudeAgent:
            assert srv["type"] == "stdio"
        else:
            assert "type" not in srv
    else:
        doc = tomlkit.parse(content)
        srv = doc[top_key]["dlt-workspace-mcp"]  # type: ignore[index]
        assert srv["command"] == "uv"
        assert "type" not in srv


def test_toolkit_install_mcp_conflict() -> None:
    """Pre-existing server names are skipped, file left untouched."""
    toolkit_dir = make_mock_toolkit(with_mcp=True)
    project_root = Path("project")
    project_root.mkdir()

    variant = _ClaudeAgent()
    config_path = project_root / ".mcp.json"
    original = json.dumps({"mcpServers": {"dlt-workspace-mcp": {"command": "existing"}}})
    config_path.write_text(original, encoding="utf-8")

    actions, _warnings = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    assert not any(a.kind == "mcp" for a in actions)

    _execute_install(actions)

    assert config_path.read_text(encoding="utf-8") == original


def test_toolkit_install_mcp_merge_existing() -> None:
    """New server merges with existing different-named servers."""
    toolkit_dir = make_mock_toolkit(with_mcp=True)
    project_root = Path("project")
    project_root.mkdir()

    variant = _ClaudeAgent()
    config_path = project_root / ".mcp.json"
    config_path.write_text(
        json.dumps({"mcpServers": {"other-server": {"command": "other"}}}),
        encoding="utf-8",
    )

    actions, _warnings = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    mcp_actions = [a for a in actions if a.kind == "mcp"]
    assert len(mcp_actions) == 1
    merged_data = json.loads(str(mcp_actions[0].content_or_path))
    assert "other-server" in merged_data["mcpServers"]
    assert "dlt-workspace-mcp" in merged_data["mcpServers"]

    _execute_install(actions)

    data = json.loads(config_path.read_text(encoding="utf-8"))
    assert "other-server" in data["mcpServers"]
    assert "dlt-workspace-mcp" in data["mcpServers"]


def test_build_dependency_map() -> None:
    """build_dependency_map reads dependencies from plugin.json."""
    base = make_mock_workbench()
    toolkits = scan_workbench_toolkits(base)
    dep_map = build_toolkits_dependency_map(toolkits)
    assert dep_map["rest-api-pipeline"] == ["init"]
    assert dep_map["sql-database"] == ["init"]
    assert dep_map["init"] == []


def test_resolve_dependencies_order() -> None:
    """resolve_dependencies returns deps in install order."""
    dep_map = {"a": ["b", "c"], "b": ["c"], "c": [], "d": []}
    assert resolve_toolkit_dependencies("a", dep_map) == ["c", "b"]


@pytest.mark.parametrize(
    ("dep_map", "start"),
    [
        ({"a": ["b"], "b": ["a"]}, "a"),
        ({"a": ["a"]}, "a"),
        ({"a": ["b"], "b": ["c"], "c": ["a"]}, "a"),
        ({"a": ["b"], "b": ["c"], "c": ["b"]}, "a"),
    ],
    ids=["pair", "self", "chain", "indirect"],
)
def test_resolve_dependencies_circular(dep_map: Dict[str, Any], start: str) -> None:
    with pytest.raises(ValueError, match="Circular"):
        resolve_toolkit_dependencies(start, dep_map)


def test_install_dependencies(capsys: pytest.CaptureFixture[str]) -> None:
    """_install_dependencies installs upstream deps that are not yet installed."""
    base = make_mock_workbench()
    project_root = Path("project")
    project_root.mkdir()
    agent = _ClaudeAgent()
    toolkits = scan_workbench_toolkits(base)

    with patch(
        "dlt._workspace.cli.ai.utils._toolkits_index_path",
        return_value=str(project_root / ".dlt" / ".toolkits"),
    ):
        _install_dependencies("rest-api-pipeline", toolkits, base, agent, project_root)

    assert (project_root / ".claude" / "rules" / "init-base.md").is_file()
    assert (project_root / ".claudeignore").is_file()
    output = capsys.readouterr().out
    assert "item(s) installed" in output


def test_install_dependencies_already_installed(capsys: pytest.CaptureFixture[str]) -> None:
    """_install_dependencies skips deps already in the index."""
    base = make_mock_workbench()
    project_root = Path("project")
    project_root.mkdir()
    agent = _ClaudeAgent()
    toolkits = scan_workbench_toolkits(base)

    with patch(
        "dlt._workspace.cli.ai.utils._toolkits_index_path",
        return_value=str(project_root / ".dlt" / ".toolkits"),
    ):
        _install_dependencies("rest-api-pipeline", toolkits, base, agent, project_root)
        capsys.readouterr()

        _install_dependencies("rest-api-pipeline", toolkits, base, agent, project_root)

    output = capsys.readouterr().out
    assert output == ""


def test_install_dependencies_no_deps(capsys: pytest.CaptureFixture[str]) -> None:
    """_install_dependencies is a no-op for toolkits without dependencies."""
    base = make_mock_workbench()
    project_root = Path("project")
    project_root.mkdir()
    agent = _ClaudeAgent()
    toolkits = scan_workbench_toolkits(base)

    _install_dependencies("init", toolkits, base, agent, project_root)
    output = capsys.readouterr().out
    assert output == ""


def test_toolkit_install_overwrite() -> None:
    """With overwrite=True, pre-existing files get replaced."""
    toolkit_dir = make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()

    variant = _ClaudeAgent()

    rule_dest = project_root / ".claude" / "rules"
    rule_dest.mkdir(parents=True)
    (rule_dest / "test-toolkit-coding.md").write_text("old content", encoding="utf-8")

    actions, _warnings = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    rule_action = next(a for a in actions if a.source_name == "coding")
    assert rule_action.conflict is True

    actions, _warnings = _plan_toolkit_install(
        toolkit_dir, variant, project_root, "test-toolkit", overwrite=True
    )
    rule_action = next(a for a in actions if a.source_name == "coding")
    assert rule_action.conflict is False

    installed = _execute_install(actions, overwrite=True)
    assert installed == 4
    new_content = (rule_dest / "test-toolkit-coding.md").read_text(encoding="utf-8")
    assert new_content != "old content"
    assert "Coding Style" in new_content


def test_toolkit_install_overwrite_mcp() -> None:
    """With overwrite=True, existing MCP server gets replaced."""
    toolkit_dir = make_mock_toolkit(with_mcp=True)
    project_root = Path("project")
    project_root.mkdir()

    variant = _ClaudeAgent()
    config_path = project_root / ".mcp.json"
    config_path.write_text(
        json.dumps({"mcpServers": {"dlt-workspace-mcp": {"command": "old-cmd"}}}),
        encoding="utf-8",
    )

    actions_no_ow, _ = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    assert not any(a.kind == "mcp" for a in actions_no_ow)

    actions_ow, _ = _plan_toolkit_install(
        toolkit_dir, variant, project_root, "test-toolkit", overwrite=True
    )
    mcp_actions = [a for a in actions_ow if a.kind == "mcp"]
    assert len(mcp_actions) == 1
    merged = json.loads(str(mcp_actions[0].content_or_path))
    assert merged["mcpServers"]["dlt-workspace-mcp"]["command"] == "uv"


def test_toolkit_install_overwrite_copytree() -> None:
    """With overwrite=True, existing skill dir gets overwritten via dirs_exist_ok."""
    toolkit_dir = make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()

    variant = _ClaudeAgent()

    existing_skill = project_root / ".claude" / "skills" / "find-source"
    existing_skill.mkdir(parents=True)
    (existing_skill / "SKILL.md").write_text("custom content", encoding="utf-8")
    (existing_skill / "extra.txt").write_text("user file", encoding="utf-8")

    actions, _warnings = _plan_toolkit_install(
        toolkit_dir, variant, project_root, "test-toolkit", overwrite=True
    )
    skill_action = next(a for a in actions if a.source_name == "find-source")
    assert skill_action.conflict is False

    _execute_install(actions, overwrite=True)

    assert (existing_skill / "SKILL.md").read_text(encoding="utf-8") != "custom content"
    assert (existing_skill / "helper.py").exists()
    assert (existing_skill / "extra.txt").read_text(encoding="utf-8") == "user file"


def test_install_tracks_files_in_index() -> None:
    """After install, the index has a files dict with correct relative paths and sha3_256 hashes."""
    toolkit_dir = make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()
    variant = _ClaudeAgent()

    actions, _ = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    assert not any(a.kind == "mcp" for a in actions)

    with patch(
        "dlt._workspace.cli.ai.utils._toolkits_index_path",
        return_value=str(project_root / ".dlt" / ".toolkits"),
    ):
        _execute_install(
            actions,
            toolkit_meta=make_mock_toolkit_info("test-toolkit", "1.0.0"),
            agent_name="claude",
            project_root=project_root,
        )
        idx = load_toolkits_index()

    entry = idx["test-toolkit"]
    assert "files" in entry
    files = entry["files"]
    assert isinstance(files, dict)
    assert len(files) > 0

    for rel_path in files:
        assert not rel_path.startswith("/")
        assert "sha3_256" in files[rel_path]
        disk_path = project_root / rel_path
        assert disk_path.is_file()
        expected = hashlib.sha3_256(disk_path.read_bytes()).hexdigest()
        assert files[rel_path]["sha3_256"] == expected

    skill_files = [p for p in files if "find-source" in p]
    assert len(skill_files) >= 2

    mcp_files = [p for p in files if ".mcp.json" in p]
    assert len(mcp_files) == 0

    assert "mcp_servers" not in entry


def test_install_tracks_mcp_server_names() -> None:
    """MCP server names appear in the mcp_servers list in the index."""
    toolkit_dir = make_mock_toolkit(with_mcp=True)
    project_root = Path("project")
    project_root.mkdir()
    variant = _ClaudeAgent()

    actions, _ = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    mcp_actions = [a for a in actions if a.kind == "mcp"]
    assert len(mcp_actions) == 1

    with patch(
        "dlt._workspace.cli.ai.utils._toolkits_index_path",
        return_value=str(project_root / ".dlt" / ".toolkits"),
    ):
        _execute_install(
            actions,
            toolkit_meta=make_mock_toolkit_info("test-toolkit", "1.0.0"),
            agent_name="claude",
            project_root=project_root,
        )
        idx = load_toolkits_index()

    entry = idx["test-toolkit"]
    assert "mcp_servers" in entry
    assert "dlt-workspace-mcp" in entry["mcp_servers"]
    assert ".mcp.json" not in entry.get("files", {})


def test_overwrite_replaces_file_index() -> None:
    """Overwrite install updates file hashes in the index."""
    toolkit_dir = make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()
    variant = _ClaudeAgent()

    with patch(
        "dlt._workspace.cli.ai.utils._toolkits_index_path",
        return_value=str(project_root / ".dlt" / ".toolkits"),
    ):
        actions, _ = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
        _execute_install(
            actions,
            toolkit_meta=make_mock_toolkit_info("test-toolkit", "1.0.0"),
            agent_name="claude",
            project_root=project_root,
        )
        idx = load_toolkits_index()
        original_files = dict(idx["test-toolkit"]["files"])

        rules_dir = toolkit_dir / "rules"
        (rules_dir / "coding.md").write_text(
            "---\ndescription: Updated rule\n---\n# Updated\nNew content.",
            encoding="utf-8",
        )

        actions2, _ = _plan_toolkit_install(
            toolkit_dir, variant, project_root, "test-toolkit", overwrite=True
        )
        _execute_install(
            actions2,
            overwrite=True,
            toolkit_meta=make_mock_toolkit_info("test-toolkit", "2.0.0"),
            agent_name="claude",
            project_root=project_root,
        )
        idx2 = load_toolkits_index()

    new_files = idx2["test-toolkit"]["files"]
    assert idx2["test-toolkit"]["version"] == "2.0.0"

    rule_paths = [p for p in new_files if "coding" in p]
    assert len(rule_paths) == 1
    rule_path = rule_paths[0]
    assert rule_path in original_files
    assert new_files[rule_path]["sha3_256"] != original_files[rule_path]["sha3_256"]

    disk_path = project_root / rule_path
    expected = hashlib.sha3_256(disk_path.read_bytes()).hexdigest()
    assert new_files[rule_path]["sha3_256"] == expected


@pytest.mark.parametrize("overwrite", [False, True], ids=["no-overwrite", "overwrite"])
def test_overlapping_toolkits(overwrite: bool) -> None:
    """Two toolkits targeting the same .claudeignore path."""
    project_root = Path("project")
    project_root.mkdir()
    variant = _ClaudeAgent()

    toolkit_a = make_mock_toolkit(toolkit_name="toolkit-a")
    toolkit_b_dir = Path("repo") / "toolkit-b"
    meta_dir = toolkit_b_dir / ".claude-plugin"
    meta_dir.mkdir(parents=True)
    (meta_dir / "plugin.json").write_text(
        json.dumps({"name": "toolkit-b", "version": "1.0.0", "description": "Toolkit B"}),
        encoding="utf-8",
    )
    (toolkit_b_dir / ".claudeignore").write_text("node_modules/\n", encoding="utf-8")
    rules_dir = toolkit_b_dir / "rules"
    rules_dir.mkdir(parents=True)
    (rules_dir / "style.md").write_text(
        "---\ndescription: Style rule\n---\n# Style\nBe consistent.", encoding="utf-8"
    )

    with patch(
        "dlt._workspace.cli.ai.utils._toolkits_index_path",
        return_value=str(project_root / ".dlt" / ".toolkits"),
    ):
        actions_a, _ = _plan_toolkit_install(toolkit_a, variant, project_root, "toolkit-a")
        _execute_install(
            actions_a,
            toolkit_meta=make_mock_toolkit_info("toolkit-a", "1.0.0"),
            agent_name="claude",
            project_root=project_root,
        )
        idx = load_toolkits_index()
        a_ignore_hash = idx["toolkit-a"]["files"][".claudeignore"]["sha3_256"]

        actions_b, _ = _plan_toolkit_install(
            toolkit_b_dir, variant, project_root, "toolkit-b", overwrite=overwrite
        )
        _execute_install(
            actions_b,
            overwrite=overwrite,
            toolkit_meta=make_mock_toolkit_info("toolkit-b", "1.0.0"),
            agent_name="claude",
            project_root=project_root,
        )
        idx = load_toolkits_index()

    assert idx["toolkit-a"]["files"][".claudeignore"]["sha3_256"] == a_ignore_hash
    disk_hash = hashlib.sha3_256((project_root / ".claudeignore").read_bytes()).hexdigest()

    if overwrite:
        b_ignore_hash = idx["toolkit-b"]["files"][".claudeignore"]["sha3_256"]
        assert disk_hash == b_ignore_hash
        assert b_ignore_hash != a_ignore_hash
        assert idx["toolkit-b"]["installed_at"] > idx["toolkit-a"]["installed_at"]
    else:
        assert disk_hash == a_ignore_hash
        assert ".claudeignore" not in idx["toolkit-b"]["files"]
        assert any("style" in p for p in idx["toolkit-b"]["files"])


def test_resolve_agent_from_init_index(capsys: pytest.CaptureFixture[str]) -> None:
    """_resolve_agent uses agent from init toolkit index, falls through without it."""
    project_root = Path("project")
    project_root.mkdir()

    with patch("dlt.common.runtime.run_context.active") as mock_ctx:
        settings_dir = str(project_root / ".dlt")
        mock_ctx.return_value.run_dir = str(project_root)
        mock_ctx.return_value.settings_dir = settings_dir
        mock_ctx.return_value.get_setting = functools.partial(os.path.join, settings_dir)
        os.makedirs(settings_dir, exist_ok=True)
        index_path = os.path.join(settings_dir, ".toolkits")

        with open(index_path, "w", encoding="utf-8") as f:
            yaml.dump({"init": {"version": "1.0.0", "agent": "cursor"}}, f)
        agent = _resolve_agent(None, project_root)
        assert agent.name == "cursor"

        with open(index_path, "w", encoding="utf-8") as f:
            yaml.dump({"init": {"version": "1.0.0"}}, f)
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("dlt._workspace.cli.ai.agents.home_dir", return_value=None),
            pytest.raises(CliCommandException),
        ):
            _resolve_agent(None, project_root)
