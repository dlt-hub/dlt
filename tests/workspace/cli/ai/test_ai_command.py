import functools
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from pytest_console_scripts import ScriptRunner

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.version import __version__ as dlt_version

from dlt._workspace.cli.ai.agents import _ClaudeAgent
from dlt._workspace.cli.ai.commands import (
    _execute_install,
    _plan_toolkit_install,
    ai_status_command,
    ai_init_command,
    ai_secrets_list_command,
    ai_secrets_update_fragment_command,
    ai_secrets_view_redacted_command,
    ai_toolkit_info_command,
    ai_toolkit_install_command,
    ai_toolkit_list_command,
)
from dlt._workspace.cli.ai.utils import (
    AI_WORKBENCH_BASE_DIR,
    build_toolkits_dependency_map,
    load_toolkits_index,
    resolve_toolkit_dependencies,
    scan_workbench_toolkits,
)
from dlt._workspace.cli.exceptions import CliCommandException

from tests.workspace.cli.ai.utils import (
    KNOWN_TOOLKITS,
    make_mock_toolkit_info,
    make_mock_workbench,
    make_versioned_workbench,
    workbench_repo,  # noqa: F401 (session-scoped fixture)
)
from tests.workspace.utils import isolated_workspace


def test_ai_status_no_toolkits(capsys: pytest.CaptureFixture[str]) -> None:
    """ai info with empty workspace shows version, warns about init and toolkits."""
    with isolated_workspace("empty"):
        ai_status_command()

    out = capsys.readouterr().out
    assert dlt_version in out
    assert "not yet initialized" in out.lower()
    assert "dlt ai init" in out.lower()
    assert "no toolkit" in out.lower()


def test_ai_status_with_toolkits(capsys: pytest.CaptureFixture[str]) -> None:
    """ai info after install shows agent and toolkit with entry skill, no init."""
    base = make_mock_workbench()
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
    os.makedirs(".dlt", exist_ok=True)
    Path(".dlt/secrets.toml").write_text('[sources]\nkey = "val"\n', encoding="utf-8")
    Path(".dlt/dev.secrets.toml").write_text('[sources]\nkey = "dev"\n', encoding="utf-8")

    ai_secrets_list_command()
    output = capsys.readouterr().out
    assert "Secret file locations:" in output
    assert "dev.secrets.toml" in output
    assert "profile: dev" in output
    assert "secrets.toml" in output
    assert "global" not in output.lower()
    assert "not found" not in output.lower()
    lines = output.strip().splitlines()
    secret_lines = [line.strip() for line in lines if "secrets.toml" in line]
    assert len(secret_lines) >= 2
    assert "dev.secrets.toml" in secret_lines[0]


def test_ai_secrets_view_redacted(capsys: pytest.CaptureFixture[str]) -> None:
    secrets_file = Path("secrets.toml")
    secrets_file.write_text(
        '[sources.my_source]\napi_key = "sk-12345"\npassword = "hunter2"\n',
        encoding="utf-8",
    )
    ai_secrets_view_redacted_command(path=str(secrets_file))
    output = capsys.readouterr().out
    assert "[sources.my_source]" in output
    assert "api_key" in output
    assert "password" in output
    assert "**********" in output
    assert "sk-12345" not in output
    assert "hunter2" not in output


def test_ai_secrets_view_redacted_missing_file(capsys: pytest.CaptureFixture[str]) -> None:
    ai_secrets_view_redacted_command(path="nonexistent.toml")
    output = capsys.readouterr().out
    assert "not found" in output.lower()


def test_ai_secrets_view_redacted_unified(capsys: pytest.CaptureFixture[str]) -> None:
    """Without --path, shows the unified merged view from SecretsTomlProvider."""
    os.makedirs(".dlt", exist_ok=True)
    Path(".dlt/secrets.toml").write_text('[sources]\nkey = "val"\n', encoding="utf-8")
    Path(".dlt/dev.secrets.toml").write_text('[destination]\npw = "secret"\n', encoding="utf-8")
    Container()[PluggableRunContext].reload_providers()

    ai_secrets_view_redacted_command()
    output = capsys.readouterr().out
    assert "[sources]" in output
    assert "[destination]" in output
    assert "val" not in output
    assert "secret" not in output
    assert "***" in output


def test_ai_secrets_update_fragment_creates_new(capsys: pytest.CaptureFixture[str]) -> None:
    secrets_file = Path("secrets.toml")
    fragment = '[destination.bigquery]\nproject_id = "my-project"\n'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(secrets_file))
    output = capsys.readouterr().out
    assert "**********" in output
    assert "my-project" not in output
    content = secrets_file.read_text(encoding="utf-8")
    assert "my-project" in content


def test_ai_secrets_update_fragment_merges_existing(capsys: pytest.CaptureFixture[str]) -> None:
    secrets_file = Path("secrets.toml")
    secrets_file.write_text('[sources.my_source]\napi_key = "existing-key"\n', encoding="utf-8")
    fragment = '[destination.postgres]\npassword = "pg-pass"\n'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(secrets_file))
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
    output = capsys.readouterr().out
    assert "secret-pw" not in output


def test_ai_secrets_update_fragment_escaped_newlines(capsys: pytest.CaptureFixture[str]) -> None:
    """Fragment with literal \\n characters (PowerShell backtick-n produces this)."""
    secrets_file = Path("secrets.toml")
    fragment = '[sources.stripe]\napi_key = "sk-test-xxxxxxxxxxxx"\n'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(secrets_file))
    content = secrets_file.read_text(encoding="utf-8")
    assert "sk-test-xxxxxxxxxxxx" in content
    assert "[sources.stripe]" in content


def test_ai_secrets_update_fragment_single_line(capsys: pytest.CaptureFixture[str]) -> None:
    """Single-line fragment with section and key on one TOML inline."""
    secrets_file = Path("secrets.toml")
    fragment = '[sources.github]\napi_key = "ghp_xxxxxxxxxxxx"\n'
    ai_secrets_update_fragment_command(fragment=fragment, path=str(secrets_file))
    content = secrets_file.read_text(encoding="utf-8")
    assert "ghp_xxxxxxxxxxxx" in content


def test_ai_secrets_update_fragment_escaped_newlines_single_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    r"""Literal \n (two chars) is converted to real newlines for cross-platform compat."""
    secrets_file = Path("secrets.toml")
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
    assert "staging-pw" not in output
    assert "[destination.postgres]" in output
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


def test_toolkit_list(capsys: pytest.CaptureFixture[str]) -> None:
    """ai_toolkit_list_command lists toolkits with descriptions."""
    base = make_mock_workbench()
    with patch("dlt._workspace.cli.ai.commands.fetch_workbench_base", return_value=base):
        ai_toolkit_list_command(location="mock://repo", branch=None)
    output = capsys.readouterr().out
    assert "Available toolkits:" in output
    assert "Installed toolkits:" not in output
    assert "rest-api-pipeline" in output
    assert "REST API source pipeline toolkit" in output
    assert "sql-database" in output
    assert "SQL database source toolkit" in output
    assert "init" in output
    assert "Init toolkit" in output
    assert "bootstrap" not in output


def test_toolkit_info(capsys: pytest.CaptureFixture[str]) -> None:
    """ai_toolkit_info_command shows toolkit components."""
    base = make_mock_workbench()
    with patch("dlt._workspace.cli.ai.utils.fetch_workbench_base", return_value=base):
        ai_toolkit_info_command(name="rest-api-pipeline", location="mock://repo", branch=None)
    output = capsys.readouterr().out
    assert "rest-api-pipeline" in output
    assert "REST API source pipeline toolkit" in output
    assert "Use find-source skill to start!" in output
    assert "Dependencies:" not in output
    assert "Skills:" in output
    assert "find-source" in output
    assert "Commands:" in output
    assert "bootstrap" in output
    assert "Rules:" in output
    assert "coding" in output
    assert "MCP servers:" in output
    assert "dlt-workspace-mcp" in output
    assert ".claudeignore" in output


def test_toolkit_info_not_found(capsys: pytest.CaptureFixture[str]) -> None:
    """ai_toolkit_info_command warns on missing toolkit."""
    base = make_mock_workbench()
    with patch("dlt._workspace.cli.ai.utils.fetch_workbench_base", return_value=base):
        ai_toolkit_info_command(name="nonexistent", location="mock://repo", branch=None)
    output = capsys.readouterr().out
    assert "not found" in output.lower()


def test_toolkit_index_lifecycle(capsys: pytest.CaptureFixture[str]) -> None:
    """Full lifecycle: install, same-version skip, version-mismatch hint, overwrite,
    zero-file install keeps old date, and dependency tracking."""
    project_root = Path("project")
    project_root.mkdir()
    base = make_versioned_workbench(version="1.0.0")

    with (
        patch("dlt.common.runtime.run_context.active") as mock_ctx,
        patch("dlt._workspace.cli.ai.commands.fetch_workbench_base", return_value=base),
    ):
        settings_dir = str(project_root / ".dlt")
        mock_ctx.return_value.run_dir = str(project_root)
        mock_ctx.return_value.settings_dir = settings_dir
        mock_ctx.return_value.get_setting = functools.partial(os.path.join, settings_dir)

        # 1. First install
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
        assert "init" in idx
        assert "files" in idx["init"]
        assert idx["init"]["version"] == "1.0.0"
        assert idx["init"]["agent"] == "claude"
        assert idx["init"]["description"] == "Init toolkit"

        # 2. Reinstall same version — early return "already installed"
        ai_toolkit_install_command(name="my-toolkit", agent="claude", location="mock://repo")
        out = capsys.readouterr().out
        assert "already installed" in out
        idx = load_toolkits_index()
        assert idx["my-toolkit"]["installed_at"] == first_date

        # 3. Bump remote version — reinstall without overwrite
        base2 = make_versioned_workbench(version="2.0.0")
        with patch("dlt._workspace.cli.ai.commands.fetch_workbench_base", return_value=base2):
            ai_toolkit_install_command(name="my-toolkit", agent="claude", location="mock://repo")
        out = capsys.readouterr().out
        assert "Use --overwrite to update" in out
        idx = load_toolkits_index()
        assert idx["my-toolkit"]["version"] == "1.0.0"
        assert idx["my-toolkit"]["installed_at"] == first_date

        # 4. Overwrite install with new version
        with patch("dlt._workspace.cli.ai.commands.fetch_workbench_base", return_value=base2):
            ai_toolkit_install_command(
                name="my-toolkit", agent="claude", location="mock://repo", overwrite=True
            )
        out = capsys.readouterr().out
        assert "item(s) installed" in out
        idx = load_toolkits_index()
        assert idx["my-toolkit"]["version"] == "2.0.0"
        assert idx["my-toolkit"]["installed_at"] != first_date
        assert idx["my-toolkit"]["agent"] == "claude"

        # 5. Install where all files conflict (0 written) — index NOT updated
        overwrite_date = idx["my-toolkit"]["installed_at"]
        variant = _ClaudeAgent()
        actions, _ = _plan_toolkit_install(
            base2 / "my-toolkit", variant, project_root, "my-toolkit"
        )
        assert all(a.conflict for a in actions)
        installed = _execute_install(
            actions, toolkit_meta=make_mock_toolkit_info("my-toolkit", "3.0.0")
        )
        assert installed == 0
        idx = load_toolkits_index()
        assert idx["my-toolkit"]["version"] == "2.0.0"
        assert idx["my-toolkit"]["installed_at"] == overwrite_date


def test_install_stores_workflow_entry_skill(capsys: pytest.CaptureFixture[str]) -> None:
    """workflow_entry_skill from toolkit.json is stored in the index after install."""
    base = make_mock_workbench()
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
        assert not idx["init"].get("workflow_entry_skill")


def test_smoke_toolkit_list(workbench_repo: str, capsys: pytest.CaptureFixture[str]) -> None:
    """List toolkits from the real workbench repo."""
    ai_toolkit_list_command(location=workbench_repo, branch=None)
    output = capsys.readouterr().out
    assert "Available toolkits:" in output
    for name in KNOWN_TOOLKITS:
        assert name in output
    assert "init" in output


@pytest.mark.parametrize("toolkit_name", KNOWN_TOOLKITS)
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
    assert (project_root / ".claude").is_dir()
    init_rules = list((project_root / ".claude" / "rules").glob("init-*.md"))
    assert len(init_rules) > 0, "init rules should be installed as dependency"
    mcp_config = project_root / ".mcp.json"
    assert mcp_config.is_file()
    mcp_data = json.loads(mcp_config.read_text())
    assert "dlt-workspace-mcp" in mcp_data["mcpServers"]
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
        ai_toolkit_install_command(
            name="rest-api-pipeline",
            agent="claude",
            location=workbench_repo,
            branch=None,
        )
        first_output = capsys.readouterr().out
        assert "already exists" not in first_output

        ai_toolkit_install_command(
            name="rest-api-pipeline",
            agent="claude",
            location=workbench_repo,
            branch=None,
        )
        skip_output = capsys.readouterr().out
        assert "already installed" in skip_output

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
    base = Path(workbench_repo) / AI_WORKBENCH_BASE_DIR
    toolkits = scan_workbench_toolkits(base)
    dep_map = build_toolkits_dependency_map(toolkits)
    for name in KNOWN_TOOLKITS:
        if name == "init":
            assert dep_map[name] == [], "init should have no dependencies"
        else:
            assert "init" in dep_map[name], "%s should depend on init" % name
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
