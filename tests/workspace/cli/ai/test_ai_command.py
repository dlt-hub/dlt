import functools
import json
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from pytest_console_scripts import ScriptRunner

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.version import __version__ as dlt_version

from dlt._workspace.cli.ai.agents import AI_AGENTS
from dlt._workspace.cli.ai.commands import (
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
    fetch_workbench_toolkits,
)
from dlt._workspace.cli._urls import DEFAULT_AI_WORKBENCH_LICENSE_URL

from tests.workspace.cli.ai.utils import (
    AGENT_NAMES,
    INSTALLABLE_TOOLKITS,
    KNOWN_TOOLKITS,
    assert_toolkit_install,
    make_mock_workbench,
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


@pytest.mark.skipif(sys.version_info < (3, 10), reason="fastmcp requires Python >=3.10")
def test_ai_init_warns_mcp(capsys: pytest.CaptureFixture[str]) -> None:
    """ai_init_command shows MCP warning when MCP server cannot start."""
    base = make_mock_workbench()
    project_root = Path("project")
    project_root.mkdir()

    def raise_mcp(*_a: Any, **_kw: Any) -> None:
        raise RuntimeError("fastmcp not installed")

    with (
        patch("dlt.common.runtime.run_context.active") as mock_ctx,
        patch("dlt._workspace.cli.ai.commands.fetch_workbench_base", return_value=base),
        patch("dlt._workspace.mcp.WorkspaceMCP", side_effect=raise_mcp),
    ):
        settings_dir = str(project_root / ".dlt")
        mock_ctx.return_value.run_dir = str(project_root)
        mock_ctx.return_value.settings_dir = settings_dir
        mock_ctx.return_value.get_setting = functools.partial(os.path.join, settings_dir)

        ai_init_command(agent="claude", location="mock://repo")

    out = capsys.readouterr().out
    assert "mcp server cannot be started" in out.lower()
    assert "dlt ai status" in out.lower()


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


def test_ai_secrets_oss_context() -> None:
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


@pytest.mark.parametrize(
    "workspace_type",
    ["empty", "legacy"],
    ids=["dlthub-workspace", "oss"],
)
def test_user_session_e2e(
    workspace_type: str,
    workbench_repo: str,
    script_runner: ScriptRunner,
) -> None:
    """End-to-end user session: status, init, list, install all, list installed."""
    required = "WorkspaceRunContext" if workspace_type == "empty" else "RunContext"
    with isolated_workspace(workspace_type, required=required):
        location_args = ["--location", workbench_repo]

        # 1. ai status — fresh workspace has warnings
        result = script_runner.run(["dlt", "ai", "status"])
        assert result.returncode == 0
        if workspace_type == "empty":
            assert "not yet initialized" in result.stdout.lower()
        assert "no toolkit" in result.stdout.lower()

        # 2. dlt ai init
        result = script_runner.run(["dlt", "ai", "init", "--agent", "claude"] + location_args)
        assert result.returncode == 0
        assert "item(s) installed" in result.stdout

        # 3. ai toolkit list — discover available toolkits
        result = script_runner.run(["dlt", "ai", "toolkit", "list"] + location_args)
        assert result.returncode == 0
        assert "Available toolkits:" in result.stdout
        for name in KNOWN_TOOLKITS:
            assert name in result.stdout

        # 4. install all installable toolkits
        base = Path(workbench_repo) / AI_WORKBENCH_BASE_DIR
        available = fetch_workbench_toolkits(base, listed_only=True)
        installable = [name for name in available if name != "init"]

        for toolkit_name in installable:
            result = script_runner.run(
                ["dlt", "ai", "toolkit", toolkit_name, "install", "--agent", "claude"]
                + location_args
            )
            assert result.returncode == 0, "install %s failed: %s" % (toolkit_name, result.stderr)

        # 5. ai toolkit list again — shows installed toolkits
        result = script_runner.run(["dlt", "ai", "toolkit", "list"] + location_args)
        assert result.returncode == 0
        assert "Installed toolkits:" in result.stdout
        for toolkit_name in installable:
            assert toolkit_name in result.stdout

        # 6. verify index and installed files
        project_root = Path.cwd()
        assert_toolkit_install(project_root, "init", "claude")
        for toolkit_name in installable:
            assert_toolkit_install(project_root, toolkit_name, "claude")


def test_toolkit_list_workbench(workbench_repo: str, capsys: pytest.CaptureFixture[str]) -> None:
    """List toolkits from the real workbench repo."""
    ai_toolkit_list_command(location=workbench_repo, branch=None)
    output = capsys.readouterr().out
    assert "Available toolkits:" in output
    for name in KNOWN_TOOLKITS:
        assert name in output
    assert "init" in output


@pytest.mark.parametrize("toolkit_name", KNOWN_TOOLKITS)
def test_toolkit_info_workbench(
    toolkit_name: str, workbench_repo: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Info for each real toolkit shows its name."""
    ai_toolkit_info_command(name=toolkit_name, location=workbench_repo, branch=None)
    output = capsys.readouterr().out
    assert toolkit_name in output


@pytest.mark.parametrize("agent_name", AGENT_NAMES)
@pytest.mark.parametrize("toolkit_name", INSTALLABLE_TOOLKITS)
def test_toolkit_install_workbench(
    toolkit_name: str,
    agent_name: str,
    workbench_repo: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Install one toolkit for one agent, validate index, files, hashes."""
    project_root = Path.cwd()
    ai_toolkit_install_command(
        name=toolkit_name,
        agent=agent_name,
        location=workbench_repo,
        branch=None,
    )

    output = capsys.readouterr().out
    assert "item(s) installed" in output

    entry = assert_toolkit_install(project_root, toolkit_name, agent_name)
    if wes := entry.get("workflow_entry_skill"):
        assert wes in output, "entry skill %s should appear in output" % wes

    # init is always installed as a dependency
    assert_toolkit_install(project_root, "init", agent_name)


@pytest.mark.parametrize("agent_name", AGENT_NAMES)
def test_init_workbench(
    agent_name: str, workbench_repo: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Run ai init for each agent, validate index, files, hashes."""
    project_root = Path.cwd()
    ai_init_command(
        agent=agent_name,
        location=workbench_repo,
        branch=None,
    )

    output = capsys.readouterr().out
    assert "item(s) installed" in output
    assert_toolkit_install(project_root, "init", agent_name)


@pytest.mark.parametrize("agent_name", AGENT_NAMES)
def test_toolkit_install_overwrite_workbench(
    agent_name: str, workbench_repo: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Install, skip, overwrite flow for each agent against real repo."""
    project_root = Path.cwd()

    ai_toolkit_install_command(
        name="rest-api-pipeline",
        agent=agent_name,
        location=workbench_repo,
        branch=None,
    )
    first_output = capsys.readouterr().out
    assert "already exists" not in first_output
    assert_toolkit_install(project_root, "rest-api-pipeline", agent_name)

    ai_toolkit_install_command(
        name="rest-api-pipeline",
        agent=agent_name,
        location=workbench_repo,
        branch=None,
    )
    skip_output = capsys.readouterr().out
    assert "already installed" in skip_output

    ai_toolkit_install_command(
        name="rest-api-pipeline",
        agent=agent_name,
        location=workbench_repo,
        branch=None,
        overwrite=True,
    )
    overwrite_output = capsys.readouterr().out
    assert "already exists" not in overwrite_output
    assert "item(s) installed" in overwrite_output
    assert_toolkit_install(project_root, "rest-api-pipeline", agent_name)


def test_dependency_map_workbench(workbench_repo: str) -> None:
    """build_dependency_map reads workbench dependencies and license links."""
    repo_root = Path(workbench_repo)
    base = repo_root / AI_WORKBENCH_BASE_DIR
    toolkits = fetch_workbench_toolkits(base)
    dep_map = build_toolkits_dependency_map(toolkits)
    for name in KNOWN_TOOLKITS:
        if name == "init":
            assert dep_map[name] == [], "init should have no dependencies"
        else:
            assert "init" in dep_map[name], "%s should depend on init" % name
    for name in dep_map:
        resolve_toolkit_dependencies(name, dep_map)

    # marketplace index and every plugin.json must link to the license
    marketplace = json.loads(
        (repo_root / ".claude-plugin" / "marketplace.json").read_text(encoding="utf-8")
    )
    assert marketplace["metadata"]["license"] == DEFAULT_AI_WORKBENCH_LICENSE_URL
    for entry in marketplace["plugins"]:
        plugin_json_path = base / entry["name"] / ".claude-plugin" / "plugin.json"
        plugin = json.loads(plugin_json_path.read_text(encoding="utf-8"))
        assert plugin["license"] == DEFAULT_AI_WORKBENCH_LICENSE_URL, (
            "%s plugin.json license mismatch" % entry["name"]
        )


def test_toolkit_info_entry_skill_workbench(
    workbench_repo: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """toolkit info shows entry skill for real toolkits."""
    ai_toolkit_info_command(name="rest-api-pipeline", location=workbench_repo, branch=None)
    output = capsys.readouterr().out
    assert "Use find-source skill to start!" in output
    assert "Dependencies:" not in output


@pytest.mark.parametrize("agent_name", AGENT_NAMES)
def test_toolkit_install_all_together_workbench(
    agent_name: str, workbench_repo: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Install all listed toolkits into one project, validate each."""
    project_root = Path.cwd()

    # get available toolkits from the workbench (same source as list command)
    available = fetch_workbench_toolkits(
        Path(workbench_repo) / AI_WORKBENCH_BASE_DIR, listed_only=True
    )
    installable = [name for name in available if name != "init"]

    # hardcoded list must be a subset of what the workbench actually provides
    assert set(INSTALLABLE_TOOLKITS) <= set(
        installable
    ), "INSTALLABLE_TOOLKITS has entries not in workbench: %s" % (
        set(INSTALLABLE_TOOLKITS) - set(installable)
    )

    for toolkit_name in installable:
        ai_toolkit_install_command(
            name=toolkit_name,
            agent=agent_name,
            location=workbench_repo,
            branch=None,
        )
        capsys.readouterr()

    idx = load_toolkits_index()
    for toolkit_name in installable:
        assert toolkit_name in idx, "toolkit %s not in index after install" % toolkit_name
        assert_toolkit_install(project_root, toolkit_name, agent_name)
    assert_toolkit_install(project_root, "init", agent_name)


@pytest.mark.parametrize("agent_name", AGENT_NAMES)
def test_init_autodetect_workbench(
    agent_name: str,
    workbench_repo: str,
    environment: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """ai_init_command(agent=None) auto-detects agent via global marker."""
    project_root = Path.cwd()

    # plant exactly one agent's global marker in a fake home
    fake_home = Path("fake_home")
    fake_home.mkdir()
    marker = AI_AGENTS[agent_name]._GLOBAL_MARKER
    (fake_home / marker).mkdir()
    monkeypatch.setattr("dlt._workspace.cli.ai.agents.home_dir", lambda: fake_home)

    ai_init_command(
        agent=None,
        location=workbench_repo,
        branch=None,
    )

    output = capsys.readouterr().out
    assert "item(s) installed" in output
    assert agent_name in output
    assert_toolkit_install(project_root, "init", agent_name)
