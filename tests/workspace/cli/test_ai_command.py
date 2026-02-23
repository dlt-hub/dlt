import json
import os
import shutil
from pathlib import Path
from typing import Callable, Set, Type
from unittest.mock import patch

import pytest
import tomlkit

from dlt._workspace.cli.ai.commands import (
    DEFAULT_AI_WORKBENCH_BRANCH,
    DEFAULT_AI_WORKBENCH_REPO,
    _execute_install,
    _install_init_silently,
    _plan_toolkit_install,
    ai_init_command,
    ai_secrets_list_command,
    ai_secrets_update_fragment_command,
    ai_secrets_view_redacted_command,
    ai_toolkit_info_command,
    ai_toolkit_install_command,
    ai_toolkit_list_command,
)
from dlt._workspace.cli.ai.agents import (
    _AIAgent,
    _ClaudeAgent,
    _CodexAgent,
    _CursorAgent,
)
from dlt._workspace.cli.exceptions import CliCommandException


def test_ai_secrets_list_workspace(capsys: pytest.CaptureFixture[str]) -> None:
    """In workspace context, lists project-scoped secrets.toml paths, profile first."""
    # the autouse fixture creates an "empty" workspace with profile="dev"
    # create a secrets file so there's something to find
    os.makedirs(".dlt", exist_ok=True)
    Path(".dlt/secrets.toml").write_text('[sources]\nkey = "val"\n', encoding="utf-8")
    Path(".dlt/dev.secrets.toml").write_text('[sources]\nkey = "dev"\n', encoding="utf-8")

    ai_secrets_list_command()
    output = capsys.readouterr().out
    assert "Secret file locations:" in output
    # profile-scoped path appears
    assert "dev.secrets.toml" in output
    assert "profile: dev" in output
    # base secrets.toml appears
    assert "secrets.toml" in output
    # global locations do NOT appear
    assert "global" not in output.lower()
    # no "not found" text
    assert "not found" not in output.lower()
    # profile-scoped line appears before base line
    lines = output.strip().splitlines()
    secret_lines = [line.strip() for line in lines if "secrets.toml" in line]
    assert len(secret_lines) >= 2
    # first secret line is the profile-scoped one
    assert "dev.secrets.toml" in secret_lines[0]


def test_ai_secrets_view_redacted(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    secrets_file = tmp_path / "secrets.toml"
    secrets_file.write_text(
        '[sources.my_source]\napi_key = "sk-12345"\npassword = "hunter2"\n',
        encoding="utf-8",
    )
    ai_secrets_view_redacted_command(path=str(secrets_file))
    output = capsys.readouterr().out
    # structure preserved
    assert "[sources.my_source]" in output
    assert "api_key" in output
    assert "password" in output
    # values redacted with stars
    assert "**********" in output
    assert "sk-12345" not in output
    assert "hunter2" not in output


def test_ai_secrets_view_redacted_missing_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ai_secrets_view_redacted_command(path=str(tmp_path / "nonexistent.toml"))
    output = capsys.readouterr().out
    assert "not found" in output.lower()


def test_ai_secrets_update_fragment_creates_new(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    secrets_file = tmp_path / "secrets.toml"
    fragment = '[destination.bigquery]\nproject_id = "my-project"\n'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(secrets_file))
    output = capsys.readouterr().out
    # redacted output with stars matching length of "my-project" (10)
    assert "**********" in output
    assert "my-project" not in output
    # file was written
    content = secrets_file.read_text(encoding="utf-8")
    assert "my-project" in content


def test_ai_secrets_update_fragment_merges_existing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    secrets_file = tmp_path / "secrets.toml"
    secrets_file.write_text('[sources.my_source]\napi_key = "existing-key"\n', encoding="utf-8")
    fragment = '[destination.postgres]\npassword = "pg-pass"\n'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(secrets_file))
    # verify file has both old and new keys
    content = secrets_file.read_text(encoding="utf-8")
    assert "existing-key" in content
    assert "pg-pass" in content


def test_ai_secrets_update_fragment_invalid_toml(tmp_path: Path) -> None:
    secrets_file = tmp_path / "secrets.toml"
    with pytest.raises(CliCommandException):
        ai_secrets_update_fragment_command(
            fragment="this is [not valid toml", path=str(secrets_file)
        )


def _make_mock_toolkit(
    tmp_path: Path, toolkit_name: str = "test-toolkit", with_mcp: bool = False
) -> Path:
    """Create a mock toolkit directory with skills, commands, and rules."""
    toolkit_dir = tmp_path / "repo" / toolkit_name
    meta_dir = toolkit_dir / ".claude-plugin"
    meta_dir.mkdir(parents=True)
    (meta_dir / "plugin.json").write_text(json.dumps({"name": toolkit_name}), encoding="utf-8")

    skill_dir = toolkit_dir / "skills" / "find-source"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: find-source\n---\nFind a source.", encoding="utf-8"
    )
    (skill_dir / "helper.py").write_text("# helper code\n", encoding="utf-8")

    cmd_dir = toolkit_dir / "commands"
    cmd_dir.mkdir(parents=True)
    (cmd_dir / "bootstrap.md").write_text(
        "---\nname: Bootstrap\ndescription: Set up project\n---\n# Bootstrap\nDo stuff",
        encoding="utf-8",
    )

    rules_dir = toolkit_dir / "rules"
    rules_dir.mkdir(parents=True)
    (rules_dir / "coding.md").write_text(
        "---\nalwaysApply: true\ndescription: Coding rule\n---\n# Coding Style\nFollow these.",
        encoding="utf-8",
    )

    (toolkit_dir / ".claudeignore").write_text("secrets.toml\n*.secrets.toml\n", encoding="utf-8")

    if with_mcp:
        (toolkit_dir / "mcp.json").write_text(
            json.dumps(
                {
                    "dlt-workspace-mcp": {
                        "type": "stdio",
                        "command": "uv",
                        "args": ["run", "dlt", "workspace", "mcp", "--stdio"],
                    }
                }
            ),
            encoding="utf-8",
        )

    return toolkit_dir


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
    tmp_path: Path,
) -> None:
    """Plans and executes a full install for each variant, verifying component types and output."""
    toolkit_dir = _make_mock_toolkit(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()

    variant = variant_cls()
    actions = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")

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


def test_toolkit_install_skip_existing(tmp_path: Path) -> None:
    """Pre-existing destinations are flagged as conflicts and left untouched."""
    toolkit_dir = _make_mock_toolkit(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()

    variant = _ClaudeAgent()

    # pre-create skill to cause conflict
    existing_skill = project_root / ".claude" / "skills" / "find-source"
    existing_skill.mkdir(parents=True)
    (existing_skill / "SKILL.md").write_text("custom content", encoding="utf-8")

    actions = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")

    # conflict detected in plan
    skill_action = next(a for a in actions if a.source_name == "find-source")
    assert skill_action.conflict is True

    # execute skips conflict, installs the other three
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
    tmp_path: Path,
) -> None:
    """Plans and executes MCP install for each variant, verifying format."""
    toolkit_dir = _make_mock_toolkit(tmp_path, with_mcp=True)
    project_root = tmp_path / "project"
    project_root.mkdir()

    variant = variant_cls()
    actions = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")

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


def test_toolkit_install_mcp_conflict(tmp_path: Path) -> None:
    """Pre-existing server names are skipped, file left untouched."""
    toolkit_dir = _make_mock_toolkit(tmp_path, with_mcp=True)
    project_root = tmp_path / "project"
    project_root.mkdir()

    variant = _ClaudeAgent()
    config_path = project_root / ".mcp.json"
    original = json.dumps({"mcpServers": {"dlt-workspace-mcp": {"command": "existing"}}})
    config_path.write_text(original, encoding="utf-8")

    actions = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    # no MCP action — all servers already exist
    assert not any(a.kind == "mcp" for a in actions)

    _execute_install(actions)

    # file untouched
    assert config_path.read_text(encoding="utf-8") == original


def test_toolkit_install_mcp_merge_existing(tmp_path: Path) -> None:
    """New server merges with existing different-named servers."""
    toolkit_dir = _make_mock_toolkit(tmp_path, with_mcp=True)
    project_root = tmp_path / "project"
    project_root.mkdir()

    variant = _ClaudeAgent()
    config_path = project_root / ".mcp.json"
    config_path.write_text(
        json.dumps({"mcpServers": {"other-server": {"command": "other"}}}),
        encoding="utf-8",
    )

    actions = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    mcp_actions = [a for a in actions if a.kind == "mcp"]
    assert len(mcp_actions) == 1
    # merged content includes both old and new servers
    merged_data = json.loads(str(mcp_actions[0].content_or_path))
    assert "other-server" in merged_data["mcpServers"]
    assert "dlt-workspace-mcp" in merged_data["mcpServers"]

    _execute_install(actions)

    data = json.loads(config_path.read_text(encoding="utf-8"))
    assert "other-server" in data["mcpServers"]
    assert "dlt-workspace-mcp" in data["mcpServers"]


def _make_mock_workbench(tmp_path: Path) -> Path:
    """Create a mock workbench base dir with 2 toolkits and _init (should be skipped)."""
    base = tmp_path / "workbench"
    base.mkdir()

    # _init — should be skipped by list, but installed silently with toolkits
    init_dir = base / "_init"
    init_meta = init_dir / ".claude-plugin"
    init_meta.mkdir(parents=True)
    (init_meta / "plugin.json").write_text(
        json.dumps({"name": "_init", "description": "Init toolkit"}), encoding="utf-8"
    )
    init_rules = init_dir / "rules"
    init_rules.mkdir()
    (init_rules / "base.md").write_text(
        "---\ndescription: Base rules\n---\n# Base\nAlways follow these.", encoding="utf-8"
    )
    (init_dir / ".claudeignore").write_text("_storage\n", encoding="utf-8")

    # rest-api toolkit
    rest_dir = base / "rest-api-pipeline"
    rest_meta = rest_dir / ".claude-plugin"
    rest_meta.mkdir(parents=True)
    (rest_meta / "plugin.json").write_text(
        json.dumps(
            {
                "name": "rest-api-pipeline",
                "description": "REST API source pipeline toolkit",
            }
        ),
        encoding="utf-8",
    )
    # skill
    skill_dir = rest_dir / "skills" / "find-source"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: find-source\ndescription: Find and configure a REST API source\n---\n"
        "# Find Source\nInstructions here.",
        encoding="utf-8",
    )
    # command
    cmd_dir = rest_dir / "commands"
    cmd_dir.mkdir(parents=True)
    (cmd_dir / "bootstrap.md").write_text(
        "---\nname: bootstrap\ndescription: Set up a new REST API pipeline project\n---\n"
        "# Bootstrap\nSteps.",
        encoding="utf-8",
    )
    # rule
    rules_dir = rest_dir / "rules"
    rules_dir.mkdir(parents=True)
    (rules_dir / "coding.md").write_text(
        "---\nname: coding\ndescription: Coding style guidelines\n---\n# Coding\nFollow these.",
        encoding="utf-8",
    )
    # mcp
    (rest_dir / "mcp.json").write_text(
        json.dumps(
            {
                "dlt-workspace-mcp": {
                    "type": "stdio",
                    "command": "uv",
                    "args": ["run", "dlt", "workspace", "mcp", "--stdio"],
                }
            }
        ),
        encoding="utf-8",
    )
    # ignore
    (rest_dir / ".claudeignore").write_text("*.secrets.toml\n", encoding="utf-8")

    # sql-database toolkit (minimal)
    sql_dir = base / "sql-database"
    sql_meta = sql_dir / ".claude-plugin"
    sql_meta.mkdir(parents=True)
    (sql_meta / "plugin.json").write_text(
        json.dumps(
            {
                "name": "sql-database",
                "description": "SQL database source toolkit",
            }
        ),
        encoding="utf-8",
    )

    return base


def test_toolkit_list(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """ai_toolkit_list_command lists toolkits with descriptions."""
    base = _make_mock_workbench(tmp_path)
    with patch("dlt._workspace.cli.ai.commands._fetch_workbench_base", return_value=base):
        ai_toolkit_list_command(location="mock://repo", branch=None)
    output = capsys.readouterr().out
    assert "rest-api-pipeline" in output
    assert "REST API source pipeline toolkit" in output
    assert "sql-database" in output
    assert "SQL database source toolkit" in output
    assert "_init" not in output


def test_toolkit_info(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """ai_toolkit_info_command shows toolkit components."""
    base = _make_mock_workbench(tmp_path)
    with patch("dlt._workspace.cli.ai.commands._fetch_workbench_base", return_value=base):
        ai_toolkit_info_command(name="rest-api-pipeline", location="mock://repo", branch=None)
    output = capsys.readouterr().out
    assert "rest-api-pipeline" in output
    assert "REST API source pipeline toolkit" in output
    # skills
    assert "Skills:" in output
    assert "find-source" in output
    # commands
    assert "Commands:" in output
    assert "bootstrap" in output
    # rules
    assert "Rules:" in output
    assert "coding" in output
    # mcp
    assert "MCP servers:" in output
    assert "dlt-workspace-mcp" in output
    # ignore
    assert ".claudeignore" in output


def test_toolkit_info_not_found(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """ai_toolkit_info_command warns on missing toolkit."""
    base = _make_mock_workbench(tmp_path)
    with patch("dlt._workspace.cli.ai.commands._fetch_workbench_base", return_value=base):
        ai_toolkit_info_command(name="nonexistent", location="mock://repo", branch=None)
    output = capsys.readouterr().out
    assert "not found" in output.lower()


def test_install_init_silently(tmp_path: Path) -> None:
    """_install_init_silently installs _init components without overwriting."""
    base = _make_mock_workbench(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()
    agent = _ClaudeAgent()

    _install_init_silently(base, agent, project_root)

    # _init rule installed
    assert (project_root / ".claude" / "rules" / "_init-base.md").is_file()
    # _init ignore file installed
    assert (project_root / ".claudeignore").is_file()
    assert "_storage" in (project_root / ".claudeignore").read_text(encoding="utf-8")


def test_install_init_silently_no_overwrite(tmp_path: Path) -> None:
    """_install_init_silently does not overwrite existing files."""
    base = _make_mock_workbench(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()
    agent = _ClaudeAgent()

    # pre-create .claudeignore
    (project_root / ".claudeignore").write_text("custom content", encoding="utf-8")

    _install_init_silently(base, agent, project_root)

    # existing file untouched
    assert (project_root / ".claudeignore").read_text(encoding="utf-8") == "custom content"


def test_install_init_silently_missing_init(tmp_path: Path) -> None:
    """_install_init_silently is a no-op when _init dir doesn't exist."""
    base = tmp_path / "empty_base"
    base.mkdir()
    project_root = tmp_path / "project"
    project_root.mkdir()
    agent = _ClaudeAgent()

    # should not raise
    _install_init_silently(base, agent, project_root)


def test_toolkit_install_overwrite(tmp_path: Path) -> None:
    """With overwrite=True, pre-existing files get replaced."""
    toolkit_dir = _make_mock_toolkit(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()

    variant = _ClaudeAgent()

    # pre-create rule to cause conflict in normal mode
    rule_dest = project_root / ".claude" / "rules"
    rule_dest.mkdir(parents=True)
    (rule_dest / "test-toolkit-coding.md").write_text("old content", encoding="utf-8")

    # without overwrite — conflict detected
    actions = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    rule_action = next(a for a in actions if a.source_name == "coding")
    assert rule_action.conflict is True

    # with overwrite — no conflict
    actions = _plan_toolkit_install(
        toolkit_dir, variant, project_root, "test-toolkit", overwrite=True
    )
    rule_action = next(a for a in actions if a.source_name == "coding")
    assert rule_action.conflict is False

    installed = _execute_install(actions, overwrite=True)
    assert installed == 4
    new_content = (rule_dest / "test-toolkit-coding.md").read_text(encoding="utf-8")
    assert new_content != "old content"
    assert "Coding Style" in new_content


def test_toolkit_install_overwrite_mcp(tmp_path: Path) -> None:
    """With overwrite=True, existing MCP server gets replaced."""
    toolkit_dir = _make_mock_toolkit(tmp_path, with_mcp=True)
    project_root = tmp_path / "project"
    project_root.mkdir()

    variant = _ClaudeAgent()
    config_path = project_root / ".mcp.json"
    config_path.write_text(
        json.dumps({"mcpServers": {"dlt-workspace-mcp": {"command": "old-cmd"}}}),
        encoding="utf-8",
    )

    # without overwrite — no MCP action (server already exists)
    actions_no_ow = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    assert not any(a.kind == "mcp" for a in actions_no_ow)

    # with overwrite — MCP action included with updated server
    actions_ow = _plan_toolkit_install(
        toolkit_dir, variant, project_root, "test-toolkit", overwrite=True
    )
    mcp_actions = [a for a in actions_ow if a.kind == "mcp"]
    assert len(mcp_actions) == 1
    merged = json.loads(str(mcp_actions[0].content_or_path))
    assert merged["mcpServers"]["dlt-workspace-mcp"]["command"] == "uv"


def test_toolkit_install_overwrite_copytree(tmp_path: Path) -> None:
    """With overwrite=True, existing skill dir gets overwritten via dirs_exist_ok."""
    toolkit_dir = _make_mock_toolkit(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()

    variant = _ClaudeAgent()

    # pre-create skill dir
    existing_skill = project_root / ".claude" / "skills" / "find-source"
    existing_skill.mkdir(parents=True)
    (existing_skill / "SKILL.md").write_text("custom content", encoding="utf-8")
    (existing_skill / "extra.txt").write_text("user file", encoding="utf-8")

    actions = _plan_toolkit_install(
        toolkit_dir, variant, project_root, "test-toolkit", overwrite=True
    )
    skill_action = next(a for a in actions if a.source_name == "find-source")
    assert skill_action.conflict is False

    _execute_install(actions, overwrite=True)

    # SKILL.md replaced
    assert (existing_skill / "SKILL.md").read_text(encoding="utf-8") != "custom content"
    # helper.py from toolkit copied in
    assert (existing_skill / "helper.py").exists()
    # user's extra file preserved (dirs_exist_ok merges)
    assert (existing_skill / "extra.txt").read_text(encoding="utf-8") == "user file"


from dlt.common.libs import git

# known toolkits in the repo (excluding _init)
_KNOWN_TOOLKITS = ["bootstrap", "data-exploration", "rest-api-pipeline"]


@pytest.fixture(scope="session")
def _cached_workbench_repo(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Clone the ai workbench repo once per session."""
    cache_dir = tmp_path_factory.mktemp("cached_ai_workbench")
    storage = git.get_fresh_repo_files(
        DEFAULT_AI_WORKBENCH_REPO,
        str(cache_dir),
        branch=DEFAULT_AI_WORKBENCH_BRANCH,
    )
    return Path(storage.storage_path)


@pytest.fixture
def workbench_repo(_cached_workbench_repo: Path, tmp_path: Path) -> str:
    """Copy the cached clone into a temp dir and return the path as a local location."""
    target = tmp_path / "workbench_copy"
    shutil.copytree(_cached_workbench_repo, target)
    return str(target)


def test_smoke_toolkit_list(workbench_repo: str, capsys: pytest.CaptureFixture[str]) -> None:
    """List toolkits from the real workbench repo."""
    ai_toolkit_list_command(location=workbench_repo, branch=None)
    output = capsys.readouterr().out
    assert "Available toolkits:" in output
    for name in _KNOWN_TOOLKITS:
        assert name in output
    # _init must not appear
    assert "_init" not in output


@pytest.mark.parametrize("toolkit_name", _KNOWN_TOOLKITS)
def test_smoke_toolkit_info(
    toolkit_name: str, workbench_repo: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Info for each real toolkit shows its name."""
    ai_toolkit_info_command(name=toolkit_name, location=workbench_repo, branch=None)
    output = capsys.readouterr().out
    assert toolkit_name in output


def test_smoke_toolkit_install(
    workbench_repo: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Install a real toolkit into a temp project dir."""
    project_root = tmp_path / "project"
    project_root.mkdir()

    with patch("dlt.common.runtime.run_context.active") as mock_ctx:
        mock_ctx.return_value.run_dir = str(project_root)
        ai_toolkit_install_command(
            name="rest-api-pipeline",
            agent="claude",
            location=workbench_repo,
            branch=None,
        )

    output = capsys.readouterr().out
    assert "item(s) installed" in output
    # at least some files were created
    assert (project_root / ".claude").is_dir()
    # _init components installed silently alongside the toolkit
    assert (project_root / ".claude" / "rules" / "_init-bootstrap.md").is_file()
    assert (project_root / ".claudeignore").is_file()
    # MCP server installed (from _init and/or rest-api-pipeline)
    mcp_config = project_root / ".mcp.json"
    assert mcp_config.is_file()
    mcp_data = json.loads(mcp_config.read_text())
    assert "dlt-workspace" in mcp_data["mcpServers"]
    assert "mcp" in " ".join(mcp_data["mcpServers"]["dlt-workspace"]["args"])


def test_smoke_init(
    workbench_repo: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Run ai init from the real workbench repo."""
    project_root = tmp_path / "project"
    project_root.mkdir()

    with patch("dlt.common.runtime.run_context.active") as mock_ctx:
        mock_ctx.return_value.run_dir = str(project_root)
        ai_init_command(
            agent="claude",
            location=workbench_repo,
            branch=None,
        )

    output = capsys.readouterr().out
    assert "item(s) installed" in output
    # MCP server installed from _init
    mcp_config = project_root / ".mcp.json"
    assert mcp_config.is_file()
    mcp_data = json.loads(mcp_config.read_text())
    assert "dlt-workspace" in mcp_data["mcpServers"]


def test_smoke_toolkit_install_overwrite(
    workbench_repo: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Install, then reinstall with --overwrite against real repo."""
    project_root = tmp_path / "project"
    project_root.mkdir()

    with patch("dlt.common.runtime.run_context.active") as mock_ctx:
        mock_ctx.return_value.run_dir = str(project_root)
        # first install
        ai_toolkit_install_command(
            name="rest-api-pipeline",
            agent="claude",
            location=workbench_repo,
            branch=None,
        )
        first_output = capsys.readouterr().out
        assert "Skipping" not in first_output

        # second install without overwrite — should skip
        ai_toolkit_install_command(
            name="rest-api-pipeline",
            agent="claude",
            location=workbench_repo,
            branch=None,
        )
        skip_output = capsys.readouterr().out
        assert "Skipping" in skip_output

        # third install with overwrite — no skips
        ai_toolkit_install_command(
            name="rest-api-pipeline",
            agent="claude",
            location=workbench_repo,
            branch=None,
            overwrite=True,
        )
        overwrite_output = capsys.readouterr().out
        assert "Skipping" not in overwrite_output
        assert "item(s) installed" in overwrite_output
