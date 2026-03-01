import functools
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Set, Type
from unittest.mock import patch
import pytest
import tomlkit
import yaml
from pytest_console_scripts import ScriptRunner

from dlt.common.libs import git

from dlt._workspace.cli.ai.commands import (
    _execute_install,
    _install_dependencies,
    _plan_toolkit_install,
    _report_and_execute,
    _resolve_agent,
    ai_status_command,
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
from dlt._workspace.cli.ai.utils import (
    DEFAULT_AI_WORKBENCH_BRANCH,
    DEFAULT_AI_WORKBENCH_REPO,
    build_toolkits_dependency_map,
    compute_file_hash,
    load_toolkits_index,
    resolve_toolkit_dependencies,
)
from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.typing import TToolkitInfo

from tests.workspace.utils import isolated_workspace

# known toolkits in the repo (init is now visible)
_KNOWN_TOOLKITS = ["data-exploration", "init", "rest-api-pipeline", "dlthub-runtime"]


def _mock_toolkit_info(
    name: str = "test-toolkit",
    version: str = "1.0.0",
    description: str = "",
    tags: None = None,
    workflow_entry_skill: str = "",
) -> TToolkitInfo:
    """Build a ``TToolkitInfo`` for tests."""
    meta = TToolkitInfo(
        name=name,
        version=version,
        description=description,
        tags=list(tags or []),
    )
    if workflow_entry_skill:
        meta["workflow_entry_skill"] = workflow_entry_skill
    return meta


def test_ai_status_no_toolkits(capsys: pytest.CaptureFixture[str]) -> None:
    """ai info with empty workspace shows version, warns about init and toolkits."""
    from dlt.version import __version__ as expected_version

    with isolated_workspace("empty"):
        ai_status_command()

    out = capsys.readouterr().out
    assert expected_version in out
    assert "not yet initialized" in out.lower()
    assert "dlt ai init" in out.lower()
    assert "no toolkit" in out.lower()


def test_ai_status_with_toolkits(capsys: pytest.CaptureFixture[str]) -> None:
    """ai info after install shows agent and toolkit with entry skill, no init."""
    base = _make_mock_workbench()
    project_root = Path("project")
    project_root.mkdir()

    with (
        patch("dlt.common.runtime.run_context.active") as mock_ctx,
        patch("dlt._workspace.cli.ai.commands.fetch_workbench_base", return_value=base),
    ):
        settings_dir = str(project_root / ".dlt")
        mock_ctx.return_value.run_dir = str(project_root)
        mock_ctx.return_value.settings_dir = settings_dir
        mock_ctx.return_value.get_setting = functools.partial(os.path.join, settings_dir)

        # create config.toml so workspace is "initialized"
        os.makedirs(settings_dir, exist_ok=True)
        Path(settings_dir, "config.toml").write_text("[runtime]\n", encoding="utf-8")

        ai_toolkit_install_command(name="rest-api-pipeline", agent="claude", location="mock://repo")
        capsys.readouterr()  # discard install output

        ai_status_command()

    out = capsys.readouterr().out
    assert "Agent: claude" in out
    assert "not initialized" not in out.lower()
    assert "run dlt ai init" not in out.lower()

    if sys.version_info < (3, 10):
        # fastmcp requires Python >=3.10, so the MCP warning must appear
        assert "mcp server cannot be started" in out.lower()
        assert "workspace" in out.lower(), "MCP warning should suggest workspace extras"
    else:
        assert "mcp server cannot be started" not in out.lower()
    assert "rest-api-pipeline" in out
    assert "find-source" in out
    # init toolkit should not be listed
    assert "\n  init" not in out
    assert "no toolkit" not in out.lower()


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


def test_ai_secrets_view_redacted(capsys: pytest.CaptureFixture[str]) -> None:
    secrets_file = Path("secrets.toml")
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


def test_ai_secrets_view_redacted_missing_file(capsys: pytest.CaptureFixture[str]) -> None:
    ai_secrets_view_redacted_command(path="nonexistent.toml")
    output = capsys.readouterr().out
    assert "not found" in output.lower()


def test_ai_secrets_view_redacted_unified(capsys: pytest.CaptureFixture[str]) -> None:
    """Without --path, shows the unified merged view from SecretsTomlProvider."""
    from dlt.common.configuration.container import Container
    from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext

    os.makedirs(".dlt", exist_ok=True)
    Path(".dlt/secrets.toml").write_text('[sources]\nkey = "val"\n', encoding="utf-8")
    Path(".dlt/dev.secrets.toml").write_text('[destination]\npw = "secret"\n', encoding="utf-8")
    # reload so the provider picks up files written above
    Container()[PluggableRunContext].reload_providers()

    ai_secrets_view_redacted_command()
    output = capsys.readouterr().out
    # both sections present in unified view
    assert "[sources]" in output
    assert "[destination]" in output
    # all values redacted
    assert "val" not in output
    assert "secret" not in output
    assert "***" in output


def test_ai_secrets_update_fragment_creates_new(capsys: pytest.CaptureFixture[str]) -> None:
    secrets_file = Path("secrets.toml")
    fragment = '[destination.bigquery]\nproject_id = "my-project"\n'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(secrets_file))
    output = capsys.readouterr().out
    # redacted output with stars matching length of "my-project" (10)
    assert "**********" in output
    assert "my-project" not in output
    # file was written
    content = secrets_file.read_text(encoding="utf-8")
    assert "my-project" in content


def test_ai_secrets_update_fragment_merges_existing(capsys: pytest.CaptureFixture[str]) -> None:
    secrets_file = Path("secrets.toml")
    secrets_file.write_text('[sources.my_source]\napi_key = "existing-key"\n', encoding="utf-8")
    fragment = '[destination.postgres]\npassword = "pg-pass"\n'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(secrets_file))
    # verify file has both old and new keys
    content = secrets_file.read_text(encoding="utf-8")
    assert "existing-key" in content
    assert "pg-pass" in content


def test_ai_secrets_update_fragment_invalid_toml() -> None:
    secrets_file = Path("secrets.toml")
    with pytest.raises(CliCommandException):
        ai_secrets_update_fragment_command(
            fragment="this is [not valid toml", path=str(secrets_file)
        )


def test_ai_secrets_update_fragment_multiline(capsys: pytest.CaptureFixture[str]) -> None:
    """Multiline TOML fragment with real newlines (bash heredoc style)."""
    secrets_file = Path("secrets.toml")
    fragment = (
        "[destination.postgres.credentials]\n"
        'host = "localhost"\n'
        "port = 5432\n"
        'database = "analytics"\n'
        'username = "loader"\n'
        'password = "secret-pw"\n'
    )
    ai_secrets_update_fragment_command(fragment=fragment, path=str(secrets_file))
    content = secrets_file.read_text(encoding="utf-8")
    assert "localhost" in content
    assert "5432" in content or "port" in content
    assert "analytics" in content
    assert "loader" in content
    assert "secret-pw" in content
    # redacted output hides the password
    output = capsys.readouterr().out
    assert "secret-pw" not in output


def test_ai_secrets_update_fragment_escaped_newlines(capsys: pytest.CaptureFixture[str]) -> None:
    """Fragment with literal \\n characters (PowerShell backtick-n produces this)."""
    secrets_file = Path("secrets.toml")
    # this is what arrives after PowerShell processes `n → actual newlines
    fragment = '[sources.stripe]\napi_key = "sk-test-xxxxxxxxxxxx"\n'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(secrets_file))
    content = secrets_file.read_text(encoding="utf-8")
    assert "sk-test-xxxxxxxxxxxx" in content
    assert "[sources.stripe]" in content


def test_ai_secrets_update_fragment_single_line(capsys: pytest.CaptureFixture[str]) -> None:
    """Single-line fragment with section and key on one TOML inline."""
    secrets_file = Path("secrets.toml")
    # agents sometimes send a minimal fragment
    fragment = '[sources.github]\napi_key = "ghp_xxxxxxxxxxxx"\n'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(secrets_file))
    content = secrets_file.read_text(encoding="utf-8")
    assert "ghp_xxxxxxxxxxxx" in content


def test_ai_secrets_update_fragment_escaped_newlines_single_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    r"""Literal \n (two chars) is converted to real newlines for cross-platform compat."""
    secrets_file = Path("secrets.toml")
    # this is what arrives when the agent passes a single-line string with \n
    fragment = r'[sources.stripe]\napi_key = "sk-test-xxxxxxxxxxxx"'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(secrets_file))
    content = secrets_file.read_text(encoding="utf-8")
    assert "sk-test-xxxxxxxxxxxx" in content
    assert "[sources.stripe]" in content


def test_ai_secrets_update_fragment_with_path(capsys: pytest.CaptureFixture[str]) -> None:
    """update-fragment writes to the exact file specified by --path."""
    target = Path(".dlt/prod.secrets.toml")
    fragment = '[sources.my_api]\ntoken = "secret-prod-token"\n'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(target))
    assert target.is_file()
    content = target.read_text(encoding="utf-8")
    assert "secret-prod-token" in content


def test_ai_secrets_roundtrip(capsys: pytest.CaptureFixture[str]) -> None:
    """Write via update-fragment, then read back via view-redacted."""
    custom = Path(".dlt/staging.secrets.toml")
    fragment = '[destination.postgres]\npassword = "staging-pw"\n'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(custom))
    capsys.readouterr()

    ai_secrets_view_redacted_command(path=str(custom))
    output = capsys.readouterr().out
    assert "[destination.postgres]" in output
    assert "staging-pw" not in output
    assert "***" in output


def test_ai_secrets_oss_context(
    autouse_test_storage: None,
    preserve_run_context: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """In OSS context (no profiles), update-fragment writes to given path."""
    with isolated_workspace("legacy", required="RunContext"):
        target = ".dlt/secrets.toml"
        fragment = '[sources.oss]\nkey = "oss-value"\n'
        ai_secrets_update_fragment_command(fragment=fragment, path=target)
        assert Path(target).is_file()
        content = Path(target).read_text(encoding="utf-8")
        assert "oss-value" in content


def test_cli_secrets_update_fragment_multiline(script_runner: ScriptRunner) -> None:
    """CLI: multiline fragment with real newlines (POSIX shells)."""
    result = script_runner.run(
        [
            "dlt",
            "ai",
            "secrets",
            "update-fragment",
            "--path",
            ".dlt/cli-test.secrets.toml",
            '[sources.stripe]\napi_key = "sk-cli-test"\n',
        ]
    )
    assert result.returncode == 0
    content = Path(".dlt/cli-test.secrets.toml").read_text(encoding="utf-8")
    assert "sk-cli-test" in content
    assert "[sources.stripe]" in content


def test_cli_secrets_update_fragment_escaped_newlines(script_runner: ScriptRunner) -> None:
    r"""CLI: literal \n (two chars) converted to real newlines (Windows compat)."""
    result = script_runner.run(
        [
            "dlt",
            "ai",
            "secrets",
            "update-fragment",
            "--path",
            ".dlt/cli-escaped.secrets.toml",
            r'[sources.github]\napi_key = "ghp-escaped-test"',
        ]
    )
    assert result.returncode == 0
    content = Path(".dlt/cli-escaped.secrets.toml").read_text(encoding="utf-8")
    assert "ghp-escaped-test" in content
    assert "[sources.github]" in content


def test_cli_secrets_roundtrip(script_runner: ScriptRunner) -> None:
    """CLI: write via update-fragment, read via view-redacted."""
    custom = ".dlt/cli-roundtrip.secrets.toml"
    result = script_runner.run(
        [
            "dlt",
            "ai",
            "secrets",
            "update-fragment",
            "--path",
            custom,
            '[destination.postgres.credentials]\nhost = "localhost"\npassword = "secret-pw"\n',
        ]
    )
    assert result.returncode == 0

    result = script_runner.run(["dlt", "ai", "secrets", "view-redacted", "--path", custom])
    assert result.returncode == 0
    assert "secret-pw" not in result.stdout
    assert "[destination.postgres.credentials]" in result.stdout
    assert "***" in result.stdout


def _make_mock_toolkit(toolkit_name: str = "test-toolkit", with_mcp: bool = False) -> Path:
    """Create a mock toolkit directory with skills, commands, and rules."""
    toolkit_dir = Path("repo") / toolkit_name
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
) -> None:
    """Plans and executes a full install for each variant, verifying component types and output."""
    toolkit_dir = _make_mock_toolkit()
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
    toolkit_dir = _make_mock_toolkit()
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

    # skill and command with bad frontmatter are reported
    assert any("Skipping skill find-source" in w for w in warn_list)
    assert any("Skipping command bootstrap" in w for w in warn_list)

    # remaining valid components (rule + ignore) are still planned
    assert any(a.kind == "rule" for a in actions)
    assert any(a.kind == "ignore" for a in actions)
    # broken skill and command are NOT in the plan
    assert not any(a.source_name == "find-source" for a in actions)
    assert not any(a.source_name == "bootstrap" for a in actions)


def test_toolkit_install_strict_fails_on_bad_frontmatter(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """With --strict, validation warnings cause CliCommandException."""
    toolkit_dir = _make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()

    # break a skill's frontmatter
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
    toolkit_dir = _make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()

    variant = _ClaudeAgent()

    # pre-create skill to cause conflict
    existing_skill = project_root / ".claude" / "skills" / "find-source"
    existing_skill.mkdir(parents=True)
    (existing_skill / "SKILL.md").write_text("custom content", encoding="utf-8")

    actions, _warnings = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")

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
) -> None:
    """Plans and executes MCP install for each variant, verifying format."""
    toolkit_dir = _make_mock_toolkit(with_mcp=True)
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
    toolkit_dir = _make_mock_toolkit(with_mcp=True)
    project_root = Path("project")
    project_root.mkdir()

    variant = _ClaudeAgent()
    config_path = project_root / ".mcp.json"
    original = json.dumps({"mcpServers": {"dlt-workspace-mcp": {"command": "existing"}}})
    config_path.write_text(original, encoding="utf-8")

    actions, _warnings = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    # no MCP action — all servers already exist
    assert not any(a.kind == "mcp" for a in actions)

    _execute_install(actions)

    # file untouched
    assert config_path.read_text(encoding="utf-8") == original


def test_toolkit_install_mcp_merge_existing() -> None:
    """New server merges with existing different-named servers."""
    toolkit_dir = _make_mock_toolkit(with_mcp=True)
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
    # merged content includes both old and new servers
    merged_data = json.loads(str(mcp_actions[0].content_or_path))
    assert "other-server" in merged_data["mcpServers"]
    assert "dlt-workspace-mcp" in merged_data["mcpServers"]

    _execute_install(actions)

    data = json.loads(config_path.read_text(encoding="utf-8"))
    assert "other-server" in data["mcpServers"]
    assert "dlt-workspace-mcp" in data["mcpServers"]


def _make_mock_workbench() -> Path:
    """Create a mock workbench base dir with 2 toolkits and init."""
    base = Path("workbench")
    base.mkdir()

    # init toolkit
    init_dir = base / "init"
    init_meta = init_dir / ".claude-plugin"
    init_meta.mkdir(parents=True)
    (init_meta / "plugin.json").write_text(
        json.dumps({"name": "init", "version": "1.0.0", "description": "Init toolkit"}),
        encoding="utf-8",
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
    (rest_meta / "toolkit.json").write_text(
        json.dumps({"dependencies": ["init"], "workflow_entry_skill": "find-source"}),
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
    (sql_meta / "toolkit.json").write_text(
        json.dumps({"dependencies": ["init"]}),
        encoding="utf-8",
    )

    # unlisted toolkit (listed: false)
    unlisted_dir = base / "bootstrap"
    unlisted_meta = unlisted_dir / ".claude-plugin"
    unlisted_meta.mkdir(parents=True)
    (unlisted_meta / "plugin.json").write_text(
        json.dumps(
            {
                "name": "bootstrap",
                "description": "Bootstrap toolkit",
            }
        ),
        encoding="utf-8",
    )
    (unlisted_meta / "toolkit.json").write_text(
        json.dumps({"dependencies": ["init"], "listed": False}),
        encoding="utf-8",
    )

    return base


def test_toolkit_list(capsys: pytest.CaptureFixture[str]) -> None:
    """ai_toolkit_list_command lists toolkits with descriptions."""
    base = _make_mock_workbench()
    with patch("dlt._workspace.cli.ai.utils.fetch_workbench_base", return_value=base):
        ai_toolkit_list_command(location="mock://repo", branch=None)
    output = capsys.readouterr().out
    # nothing installed — all under "Available"
    assert "Available toolkits:" in output
    assert "Installed toolkits:" not in output
    assert "rest-api-pipeline" in output
    assert "REST API source pipeline toolkit" in output
    assert "sql-database" in output
    assert "SQL database source toolkit" in output
    assert "init" in output
    assert "Init toolkit" in output
    # unlisted toolkit should not appear
    assert "bootstrap" not in output


def test_toolkit_info(capsys: pytest.CaptureFixture[str]) -> None:
    """ai_toolkit_info_command shows toolkit components."""
    base = _make_mock_workbench()
    with patch("dlt._workspace.cli.ai.utils.fetch_workbench_base", return_value=base):
        ai_toolkit_info_command(name="rest-api-pipeline", location="mock://repo", branch=None)
    output = capsys.readouterr().out
    assert "rest-api-pipeline" in output
    assert "REST API source pipeline toolkit" in output
    # entry skill
    assert "Use find-source skill to start!" in output
    # dependencies not shown
    assert "Dependencies:" not in output
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


def test_toolkit_info_not_found(capsys: pytest.CaptureFixture[str]) -> None:
    """ai_toolkit_info_command warns on missing toolkit."""
    base = _make_mock_workbench()
    with patch("dlt._workspace.cli.ai.utils.fetch_workbench_base", return_value=base):
        ai_toolkit_info_command(name="nonexistent", location="mock://repo", branch=None)
    output = capsys.readouterr().out
    assert "not found" in output.lower()


def test_build_dependency_map() -> None:
    """build_dependency_map reads dependencies from plugin.json."""
    base = _make_mock_workbench()
    dep_map = build_toolkits_dependency_map(base)
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
    base = _make_mock_workbench()
    project_root = Path("project")
    project_root.mkdir()
    agent = _ClaudeAgent()

    with patch(
        "dlt._workspace.cli.ai.utils._toolkits_index_path",
        return_value=str(project_root / ".dlt" / ".toolkits"),
    ):
        _install_dependencies("rest-api-pipeline", base, agent, project_root)

    # init dep installed
    assert (project_root / ".claude" / "rules" / "init-base.md").is_file()
    assert (project_root / ".claudeignore").is_file()
    output = capsys.readouterr().out
    assert "item(s) installed" in output


def test_install_dependencies_already_installed(capsys: pytest.CaptureFixture[str]) -> None:
    """_install_dependencies skips deps already in the index."""
    base = _make_mock_workbench()
    project_root = Path("project")
    project_root.mkdir()
    agent = _ClaudeAgent()

    with patch(
        "dlt._workspace.cli.ai.utils._toolkits_index_path",
        return_value=str(project_root / ".dlt" / ".toolkits"),
    ):
        # first install
        _install_dependencies("rest-api-pipeline", base, agent, project_root)
        capsys.readouterr()

        # second call — already in index, should be silent
        _install_dependencies("rest-api-pipeline", base, agent, project_root)

    output = capsys.readouterr().out
    assert output == ""


def test_install_dependencies_no_deps(capsys: pytest.CaptureFixture[str]) -> None:
    """_install_dependencies is a no-op for toolkits without dependencies."""
    base = _make_mock_workbench()
    project_root = Path("project")
    project_root.mkdir()
    agent = _ClaudeAgent()

    _install_dependencies("init", base, agent, project_root)
    output = capsys.readouterr().out
    assert output == ""


def test_toolkit_install_overwrite() -> None:
    """With overwrite=True, pre-existing files get replaced."""
    toolkit_dir = _make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()

    variant = _ClaudeAgent()

    # pre-create rule to cause conflict in normal mode
    rule_dest = project_root / ".claude" / "rules"
    rule_dest.mkdir(parents=True)
    (rule_dest / "test-toolkit-coding.md").write_text("old content", encoding="utf-8")

    # without overwrite — conflict detected
    actions, _warnings = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    rule_action = next(a for a in actions if a.source_name == "coding")
    assert rule_action.conflict is True

    # with overwrite — no conflict
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
    toolkit_dir = _make_mock_toolkit(with_mcp=True)
    project_root = Path("project")
    project_root.mkdir()

    variant = _ClaudeAgent()
    config_path = project_root / ".mcp.json"
    config_path.write_text(
        json.dumps({"mcpServers": {"dlt-workspace-mcp": {"command": "old-cmd"}}}),
        encoding="utf-8",
    )

    # without overwrite — no MCP action (server already exists)
    actions_no_ow, _ = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    assert not any(a.kind == "mcp" for a in actions_no_ow)

    # with overwrite — MCP action included with updated server
    actions_ow, _ = _plan_toolkit_install(
        toolkit_dir, variant, project_root, "test-toolkit", overwrite=True
    )
    mcp_actions = [a for a in actions_ow if a.kind == "mcp"]
    assert len(mcp_actions) == 1
    merged = json.loads(str(mcp_actions[0].content_or_path))
    assert merged["mcpServers"]["dlt-workspace-mcp"]["command"] == "uv"


def test_toolkit_install_overwrite_copytree() -> None:
    """With overwrite=True, existing skill dir gets overwritten via dirs_exist_ok."""
    toolkit_dir = _make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()

    variant = _ClaudeAgent()

    # pre-create skill dir
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

    # SKILL.md replaced
    assert (existing_skill / "SKILL.md").read_text(encoding="utf-8") != "custom content"
    # helper.py from toolkit copied in
    assert (existing_skill / "helper.py").exists()
    # user's extra file preserved (dirs_exist_ok merges)
    assert (existing_skill / "extra.txt").read_text(encoding="utf-8") == "user file"


def _make_versioned_workbench(version: str = "1.0.0") -> Path:
    """Create a mock workbench with init and a versioned toolkit."""
    base = Path("workbench")
    base.mkdir(exist_ok=True)

    # init toolkit
    init_dir = base / "init"
    init_meta = init_dir / ".claude-plugin"
    init_meta.mkdir(parents=True, exist_ok=True)
    (init_meta / "plugin.json").write_text(
        json.dumps({"name": "init", "version": "1.0.0", "description": "Init toolkit"}),
        encoding="utf-8",
    )
    init_rules = init_dir / "rules"
    init_rules.mkdir(exist_ok=True)
    (init_rules / "base.md").write_text(
        "---\ndescription: Base rules\n---\n# Base\nAlways follow these.", encoding="utf-8"
    )

    # versioned toolkit
    tk_dir = base / "my-toolkit"
    tk_meta = tk_dir / ".claude-plugin"
    tk_meta.mkdir(parents=True, exist_ok=True)
    (tk_meta / "plugin.json").write_text(
        json.dumps(
            {
                "name": "my-toolkit",
                "version": version,
                "description": "Test toolkit",
                "keywords": ["testing", "dlt"],
                "dependencies": ["init"],
            }
        ),
        encoding="utf-8",
    )
    rules_dir = tk_dir / "rules"
    rules_dir.mkdir(exist_ok=True)
    (rules_dir / "coding.md").write_text(
        "---\ndescription: Coding rule\n---\n# Coding\nFollow these.", encoding="utf-8"
    )

    return base


def test_toolkit_index_lifecycle(capsys: pytest.CaptureFixture[str]) -> None:
    """Full lifecycle: install, same-version skip, version-mismatch hint, overwrite,
    zero-file install keeps old date, and dependency tracking."""
    project_root = Path("project")
    project_root.mkdir()
    base = _make_versioned_workbench(version="1.0.0")

    with (
        patch("dlt.common.runtime.run_context.active") as mock_ctx,
        patch("dlt._workspace.cli.ai.commands.fetch_workbench_base", return_value=base),
    ):
        settings_dir = str(project_root / ".dlt")
        mock_ctx.return_value.run_dir = str(project_root)
        mock_ctx.return_value.settings_dir = settings_dir
        mock_ctx.return_value.get_setting = functools.partial(os.path.join, settings_dir)

        # 1. First install — index created with version + installed_at + description + tags
        ai_toolkit_install_command(name="my-toolkit", agent="claude", location="mock://repo")
        out = capsys.readouterr().out
        assert "item(s) installed" in out
        idx = load_toolkits_index()
        assert "my-toolkit" in idx
        assert idx["my-toolkit"]["version"] == "1.0.0"
        assert "installed_at" in idx["my-toolkit"]
        assert idx["my-toolkit"]["agent"] == "claude"
        assert idx["my-toolkit"]["description"] == "Test toolkit"
        assert idx["my-toolkit"]["tags"] == ["testing", "dlt"]
        assert "files" in idx["my-toolkit"]
        assert isinstance(idx["my-toolkit"]["files"], dict)
        assert len(idx["my-toolkit"]["files"]) > 0
        first_date = idx["my-toolkit"]["installed_at"]
        # init tracked too (via dependency resolution)
        assert "init" in idx
        assert "files" in idx["init"]
        assert idx["init"]["version"] == "1.0.0"
        assert idx["init"]["agent"] == "claude"
        assert idx["init"]["description"] == "Init toolkit"

        # 2. Reinstall same version (no overwrite) — early return "already installed"
        ai_toolkit_install_command(name="my-toolkit", agent="claude", location="mock://repo")
        out = capsys.readouterr().out
        assert "already installed" in out
        idx = load_toolkits_index()
        assert idx["my-toolkit"]["installed_at"] == first_date  # unchanged

        # 3. Bump remote version in mock meta — reinstall without overwrite
        base2 = _make_versioned_workbench(version="2.0.0")
        with patch("dlt._workspace.cli.ai.commands.fetch_workbench_base", return_value=base2):
            ai_toolkit_install_command(name="my-toolkit", agent="claude", location="mock://repo")
        out = capsys.readouterr().out
        assert "Use --overwrite to update" in out
        idx = load_toolkits_index()
        assert idx["my-toolkit"]["version"] == "1.0.0"  # still old version
        assert idx["my-toolkit"]["installed_at"] == first_date

        # 4. Overwrite install with new version — index updated
        with patch("dlt._workspace.cli.ai.commands.fetch_workbench_base", return_value=base2):
            ai_toolkit_install_command(
                name="my-toolkit", agent="claude", location="mock://repo", overwrite=True
            )
        out = capsys.readouterr().out
        assert "item(s) installed" in out
        idx = load_toolkits_index()
        assert idx["my-toolkit"]["version"] == "2.0.0"
        assert idx["my-toolkit"]["installed_at"] != first_date  # updated
        assert idx["my-toolkit"]["agent"] == "claude"

        # 5. Install where all files conflict (0 written) — index NOT updated
        overwrite_date = idx["my-toolkit"]["installed_at"]
        #  re-install v2 without overwrite but files already there → all conflict
        #  however our version check will short-circuit. So we test _execute_install directly
        variant = _ClaudeAgent()
        actions, _ = _plan_toolkit_install(
            base2 / "my-toolkit", variant, project_root, "my-toolkit"
        )
        # all actions should conflict since files exist
        assert all(a.conflict for a in actions)
        installed = _execute_install(
            actions, toolkit_meta=_mock_toolkit_info("my-toolkit", "3.0.0")
        )
        assert installed == 0
        idx = load_toolkits_index()
        # version and date unchanged because 0 files written
        assert idx["my-toolkit"]["version"] == "2.0.0"
        assert idx["my-toolkit"]["installed_at"] == overwrite_date


def test_install_stores_workflow_entry_skill(capsys: pytest.CaptureFixture[str]) -> None:
    """workflow_entry_skill from toolkit.json is stored in the index after install."""
    base = _make_mock_workbench()
    project_root = Path("project")
    project_root.mkdir()

    with (
        patch("dlt.common.runtime.run_context.active") as mock_ctx,
        patch("dlt._workspace.cli.ai.commands.fetch_workbench_base", return_value=base),
    ):
        settings_dir = str(project_root / ".dlt")
        mock_ctx.return_value.run_dir = str(project_root)
        mock_ctx.return_value.settings_dir = settings_dir
        mock_ctx.return_value.get_setting = functools.partial(os.path.join, settings_dir)

        ai_toolkit_install_command(name="rest-api-pipeline", agent="claude", location="mock://repo")

        out = capsys.readouterr().out
        assert "Use find-source skill to start!" in out

        idx = load_toolkits_index()
        assert "rest-api-pipeline" in idx
        assert idx["rest-api-pipeline"]["workflow_entry_skill"] == "find-source"
        # init has no workflow_entry_skill (empty string)
        assert not idx["init"].get("workflow_entry_skill")


def test_install_tracks_files_in_index() -> None:
    """After install, the index has a files dict with correct relative paths and sha3_256 hashes."""
    import hashlib

    toolkit_dir = _make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()
    variant = _ClaudeAgent()

    actions, _ = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
    # no MCP actions in this toolkit
    assert not any(a.kind == "mcp" for a in actions)

    with patch(
        "dlt._workspace.cli.ai.utils._toolkits_index_path",
        return_value=str(project_root / ".dlt" / ".toolkits"),
    ):
        _execute_install(
            actions,
            toolkit_meta=_mock_toolkit_info("test-toolkit", "1.0.0"),
            agent_name="claude",
            project_root=project_root,
        )
        idx = load_toolkits_index()

    entry = idx["test-toolkit"]
    assert "files" in entry
    files = entry["files"]
    assert isinstance(files, dict)
    assert len(files) > 0

    # all paths are relative (no leading /)
    for rel_path in files:
        assert not rel_path.startswith("/")
        # each entry has sha3_256
        assert "sha3_256" in files[rel_path]
        # hash matches the file on disk
        disk_path = project_root / rel_path
        assert disk_path.is_file()
        expected = hashlib.sha3_256(disk_path.read_bytes()).hexdigest()
        assert files[rel_path]["sha3_256"] == expected

    # skill copytree: individual files tracked (SKILL.md and helper.py)
    skill_files = [p for p in files if "find-source" in p]
    assert len(skill_files) >= 2  # at least SKILL.md + helper.py

    # MCP config should NOT be in files
    mcp_files = [p for p in files if ".mcp.json" in p]
    assert len(mcp_files) == 0

    # no mcp_servers key when toolkit has no MCP
    assert "mcp_servers" not in entry


def test_install_tracks_mcp_server_names() -> None:
    """MCP server names appear in the mcp_servers list in the index."""
    toolkit_dir = _make_mock_toolkit(with_mcp=True)
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
            toolkit_meta=_mock_toolkit_info("test-toolkit", "1.0.0"),
            agent_name="claude",
            project_root=project_root,
        )
        idx = load_toolkits_index()

    entry = idx["test-toolkit"]
    assert "mcp_servers" in entry
    assert "dlt-workspace-mcp" in entry["mcp_servers"]
    # MCP config file path should NOT appear in files dict
    assert ".mcp.json" not in entry.get("files", {})


def test_overwrite_replaces_file_index() -> None:
    """Overwrite install updates file hashes in the index."""
    import hashlib

    toolkit_dir = _make_mock_toolkit()
    project_root = Path("project")
    project_root.mkdir()
    variant = _ClaudeAgent()

    with patch(
        "dlt._workspace.cli.ai.utils._toolkits_index_path",
        return_value=str(project_root / ".dlt" / ".toolkits"),
    ):
        # first install
        actions, _ = _plan_toolkit_install(toolkit_dir, variant, project_root, "test-toolkit")
        _execute_install(
            actions,
            toolkit_meta=_mock_toolkit_info("test-toolkit", "1.0.0"),
            agent_name="claude",
            project_root=project_root,
        )
        idx = load_toolkits_index()
        original_files = dict(idx["test-toolkit"]["files"])

        # modify source rule content and reinstall with overwrite
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
            toolkit_meta=_mock_toolkit_info("test-toolkit", "2.0.0"),
            agent_name="claude",
            project_root=project_root,
        )
        idx2 = load_toolkits_index()

    new_files = idx2["test-toolkit"]["files"]
    assert idx2["test-toolkit"]["version"] == "2.0.0"

    # the rule file hash should have changed
    rule_paths = [p for p in new_files if "coding" in p]
    assert len(rule_paths) == 1
    rule_path = rule_paths[0]
    assert rule_path in original_files
    assert new_files[rule_path]["sha3_256"] != original_files[rule_path]["sha3_256"]

    # verify hash matches disk
    disk_path = project_root / rule_path
    expected = hashlib.sha3_256(disk_path.read_bytes()).hexdigest()
    assert new_files[rule_path]["sha3_256"] == expected


@pytest.mark.parametrize("overwrite", [False, True], ids=["no-overwrite", "overwrite"])
def test_overlapping_toolkits(overwrite: bool) -> None:
    """Two toolkits targeting the same .claudeignore path.

    Without overwrite: B's conflicting file is skipped, A's content and hash stay.
    With overwrite: B replaces the file, B's index has new hash, A's index keeps stale hash.
    In both cases installed_at lets consumers determine which entry is authoritative.
    """
    import hashlib

    project_root = Path("project")
    project_root.mkdir()
    variant = _ClaudeAgent()

    toolkit_a = _make_mock_toolkit(toolkit_name="toolkit-a")
    toolkit_b_dir = Path("repo") / "toolkit-b"
    meta_dir = toolkit_b_dir / ".claude-plugin"
    meta_dir.mkdir(parents=True)
    (meta_dir / "plugin.json").write_text(
        json.dumps({"name": "toolkit-b", "version": "1.0.0"}), encoding="utf-8"
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
        # install A
        actions_a, _ = _plan_toolkit_install(toolkit_a, variant, project_root, "toolkit-a")
        _execute_install(
            actions_a,
            toolkit_meta=_mock_toolkit_info("toolkit-a", "1.0.0"),
            agent_name="claude",
            project_root=project_root,
        )
        idx = load_toolkits_index()
        a_ignore_hash = idx["toolkit-a"]["files"][".claudeignore"]["sha3_256"]

        # install B
        actions_b, _ = _plan_toolkit_install(
            toolkit_b_dir, variant, project_root, "toolkit-b", overwrite=overwrite
        )
        _execute_install(
            actions_b,
            overwrite=overwrite,
            toolkit_meta=_mock_toolkit_info("toolkit-b", "1.0.0"),
            agent_name="claude",
            project_root=project_root,
        )
        idx = load_toolkits_index()

    # A's index always keeps its original hash
    assert idx["toolkit-a"]["files"][".claudeignore"]["sha3_256"] == a_ignore_hash
    disk_hash = hashlib.sha3_256((project_root / ".claudeignore").read_bytes()).hexdigest()

    if overwrite:
        # disk has B's content, B's index matches disk, differs from A
        b_ignore_hash = idx["toolkit-b"]["files"][".claudeignore"]["sha3_256"]
        assert disk_hash == b_ignore_hash
        assert b_ignore_hash != a_ignore_hash
        assert idx["toolkit-b"]["installed_at"] > idx["toolkit-a"]["installed_at"]
    else:
        # disk still has A's content, B skipped the conflicting file
        assert disk_hash == a_ignore_hash
        assert ".claudeignore" not in idx["toolkit-b"]["files"]
        # B's unique rule IS tracked
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

        # init entry with agent → resolves directly
        with open(index_path, "w", encoding="utf-8") as f:
            yaml.dump({"init": {"version": "1.0.0", "agent": "cursor"}}, f)
        agent = _resolve_agent(None, project_root)
        assert agent.name == "cursor"

        # init entry without agent → falls through to detect_all
        with open(index_path, "w", encoding="utf-8") as f:
            yaml.dump({"init": {"version": "1.0.0"}}, f)
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("dlt._workspace.cli.ai.agents.home_dir", return_value=None),
            pytest.raises(CliCommandException),
        ):
            _resolve_agent(None, project_root)


@pytest.fixture(scope="session")
def workbench_repo(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Clone the ai workbench repo once per session."""
    cache_dir = tmp_path_factory.mktemp("cached_ai_workbench")
    storage = git.get_fresh_repo_files(
        DEFAULT_AI_WORKBENCH_REPO,
        str(cache_dir),  # this will be ignored if DEFAULT_AI_WORKBENCH_REPO is a local folder
        branch=DEFAULT_AI_WORKBENCH_BRANCH,
    )
    # NOTE: make another copy if DEFAULT_AI_WORKBENCH_REPO is a local folder not to drop it
    target = cache_dir / "workbench_copy"
    shutil.copytree(Path(storage.storage_path), target)
    return str(target)


def test_smoke_toolkit_list(workbench_repo: str, capsys: pytest.CaptureFixture[str]) -> None:
    """List toolkits from the real workbench repo."""
    ai_toolkit_list_command(location=workbench_repo, branch=None)
    output = capsys.readouterr().out
    assert "Available toolkits:" in output
    for name in _KNOWN_TOOLKITS:
        assert name in output
    assert "init" in output


@pytest.mark.parametrize("toolkit_name", _KNOWN_TOOLKITS)
def test_smoke_toolkit_info(
    toolkit_name: str, workbench_repo: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Info for each real toolkit shows its name."""
    ai_toolkit_info_command(name=toolkit_name, location=workbench_repo, branch=None)
    output = capsys.readouterr().out
    assert toolkit_name in output


def test_smoke_toolkit_install(workbench_repo: str, capsys: pytest.CaptureFixture[str]) -> None:
    """Install a real toolkit into a temp project dir."""
    project_root = Path("project")
    project_root.mkdir()

    with patch("dlt.common.runtime.run_context.active") as mock_ctx:
        settings_dir = str(project_root / ".dlt")
        mock_ctx.return_value.run_dir = str(project_root)
        mock_ctx.return_value.settings_dir = settings_dir
        mock_ctx.return_value.get_setting = functools.partial(os.path.join, settings_dir)
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
    # init dependency installed alongside the toolkit
    init_rules = list((project_root / ".claude" / "rules").glob("init-*.md"))
    assert len(init_rules) > 0, "init rules should be installed as dependency"
    # MCP server installed (from init and/or rest-api-pipeline)
    mcp_config = project_root / ".mcp.json"
    assert mcp_config.is_file()
    mcp_data = json.loads(mcp_config.read_text())
    assert "dlt-workspace-mcp" in mcp_data["mcpServers"]
    # toolkit tracked in .dlt/.toolkits
    toolkits_file = project_root / ".dlt" / ".toolkits"
    assert toolkits_file.is_file()
    toolkits_data = yaml.safe_load(toolkits_file.read_text())
    assert "rest-api-pipeline" in toolkits_data
    assert "version" in toolkits_data["rest-api-pipeline"]
    assert "installed_at" in toolkits_data["rest-api-pipeline"]
    assert toolkits_data["rest-api-pipeline"]["agent"] == "claude"
    assert toolkits_data["rest-api-pipeline"]["workflow_entry_skill"] == "find-source"


def test_smoke_init(workbench_repo: str, capsys: pytest.CaptureFixture[str]) -> None:
    """Run ai init from the real workbench repo."""
    project_root = Path("project")
    project_root.mkdir()

    with patch("dlt.common.runtime.run_context.active") as mock_ctx:
        settings_dir = str(project_root / ".dlt")
        mock_ctx.return_value.run_dir = str(project_root)
        mock_ctx.return_value.settings_dir = settings_dir
        mock_ctx.return_value.get_setting = functools.partial(os.path.join, settings_dir)
        ai_init_command(
            agent="claude",
            location=workbench_repo,
            branch=None,
        )

    output = capsys.readouterr().out
    assert "item(s) installed" in output
    # MCP server installed from init toolkit
    mcp_config = project_root / ".mcp.json"
    assert mcp_config.is_file()
    mcp_data = json.loads(mcp_config.read_text())
    assert "dlt-workspace-mcp" in mcp_data["mcpServers"]


def test_smoke_toolkit_install_overwrite(
    workbench_repo: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Install, then reinstall with --overwrite against real repo."""
    project_root = Path("project")
    project_root.mkdir()

    with patch("dlt.common.runtime.run_context.active") as mock_ctx:
        settings_dir = str(project_root / ".dlt")
        mock_ctx.return_value.run_dir = str(project_root)
        mock_ctx.return_value.settings_dir = settings_dir
        mock_ctx.return_value.get_setting = functools.partial(os.path.join, settings_dir)
        # first install
        ai_toolkit_install_command(
            name="rest-api-pipeline",
            agent="claude",
            location=workbench_repo,
            branch=None,
        )
        first_output = capsys.readouterr().out
        # first install may warn about invalid frontmatter but should not skip existing files
        assert "already exists" not in first_output

        # second install without overwrite — already installed (same version)
        ai_toolkit_install_command(
            name="rest-api-pipeline",
            agent="claude",
            location=workbench_repo,
            branch=None,
        )
        skip_output = capsys.readouterr().out
        assert "already installed" in skip_output

        # third install with overwrite — no skips
        ai_toolkit_install_command(
            name="rest-api-pipeline",
            agent="claude",
            location=workbench_repo,
            branch=None,
            overwrite=True,
        )
        overwrite_output = capsys.readouterr().out
        assert "already exists" not in overwrite_output
        assert "item(s) installed" in overwrite_output


def test_smoke_dependency_map(workbench_repo: str) -> None:
    """build_dependency_map reads workbench dependencies."""
    from dlt._workspace.cli.ai.utils import AI_WORKBENCH_BASE_DIR

    base = Path(workbench_repo) / AI_WORKBENCH_BASE_DIR
    dep_map = build_toolkits_dependency_map(base)
    # all non-init toolkits depend on init
    for name in _KNOWN_TOOLKITS:
        if name == "init":
            assert dep_map[name] == [], "init should have no dependencies"
        else:
            assert "init" in dep_map[name], "%s should depend on init" % name
    # no circular deps — resolve_dependencies must not raise
    for name in dep_map:
        resolve_toolkit_dependencies(name, dep_map)


def test_smoke_toolkit_info_shows_entry_skill(
    workbench_repo: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """toolkit info shows entry skill for real toolkits."""
    ai_toolkit_info_command(name="rest-api-pipeline", location=workbench_repo, branch=None)
    output = capsys.readouterr().out
    assert "Use find-source skill to start!" in output
    assert "Dependencies:" not in output
