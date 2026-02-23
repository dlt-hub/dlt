from os import environ
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type

import pytest

from dlt._workspace.cli.ai.agents import (
    AI_AGENTS,
    _AIAgent,
    _ClaudeAgent,
    _CodexAgent,
    _CursorAgent,
)
from dlt._workspace.cli.formatters import parse_frontmatter


@pytest.fixture()
def environment() -> Iterator[Any]:
    """Clear environ for the test, restore via autouse preserve_environ."""
    environ.clear()
    yield environ


@pytest.mark.parametrize(
    ("variant_name", "expected_skill", "expected_command", "expected_rule"),
    [
        ("claude", ".claude/skills", ".claude/commands", ".claude/rules"),
        ("cursor", ".cursor/skills", ".cursor/commands", ".cursor/rules"),
        ("codex", ".agents/skills", None, None),
    ],
    ids=["claude", "cursor", "codex"],
)
def test_variant_component_dir(
    variant_name: str,
    expected_skill: str,
    expected_command: str,
    expected_rule: str,
    tmp_path: Path,
) -> None:
    variant = AI_AGENTS[variant_name]()
    assert variant.component_dir("skill", tmp_path) == tmp_path / expected_skill

    if expected_command is not None:
        assert variant.component_dir("command", tmp_path) == tmp_path / expected_command
    else:
        with pytest.raises(NotImplementedError):
            variant.component_dir("command", tmp_path)

    if expected_rule is not None:
        assert variant.component_dir("rule", tmp_path) == tmp_path / expected_rule
    else:
        with pytest.raises(NotImplementedError):
            variant.component_dir("rule", tmp_path)

    # ignore always resolves to project root
    assert variant.component_dir("ignore", tmp_path) == tmp_path


@pytest.mark.parametrize(
    ("variant_cls", "component_type", "content", "source_name", "toolkit_name", "exp"),
    [
        # claude: skill passthrough
        (_ClaudeAgent, "skill", "content", "my-skill", "p", ("skill", "content", "my-skill")),
        # claude: command passthrough with .md suffix
        (_ClaudeAgent, "command", "# Do", "bootstrap", "p", ("command", "# Do", "bootstrap.md")),
        # cursor: skill passthrough
        (_CursorAgent, "skill", "content", "my-skill", "p", ("skill", "content", "my-skill")),
        # cursor: command passthrough with .md suffix
        (_CursorAgent, "command", "# Do", "bootstrap", "p", ("command", "# Do", "bootstrap.md")),
        # codex: skill passthrough
        (_CodexAgent, "skill", "content", "my-skill", "p", ("skill", "content", "my-skill")),
        # ignore: each variant maps to its own ignore file name
        (_ClaudeAgent, "ignore", "*.secret", "x", "p", ("ignore", "*.secret", ".claudeignore")),
        (_CursorAgent, "ignore", "*.secret", "x", "p", ("ignore", "*.secret", ".cursorignore")),
        (_CodexAgent, "ignore", "*.secret", "x", "p", ("ignore", "*.secret", ".codexignore")),
    ],
    ids=[
        "claude-skill",
        "claude-command",
        "cursor-skill",
        "cursor-command",
        "codex-skill",
        "claude-ignore",
        "cursor-ignore",
        "codex-ignore",
    ],
)
def test_transform_passthrough(
    variant_cls: Type[_AIAgent],
    component_type: str,
    content: str,
    source_name: str,
    toolkit_name: str,
    exp: Tuple[str, str, str],
) -> None:
    """Skill passthrough and simple command transforms share logic across variants."""
    assert variant_cls().transform(component_type, content, source_name, toolkit_name) == exp  # type: ignore[arg-type]


def test_claude_transform_rule() -> None:
    variant = _ClaudeAgent()

    # strips non-Claude frontmatter keys, keeps name/description
    content = "---\nalwaysApply: true\ndescription: Cursor rule\nname: keep-me\n---\n# Rule"
    out_type, out_content, out_name = variant.transform("rule", content, "coding", "my-toolkit")
    assert (out_type, out_name) == ("rule", "my-toolkit-coding.md")
    fm, body = parse_frontmatter(out_content)
    assert "alwaysApply" not in fm
    assert fm.get("name") == "keep-me"
    assert body == "# Rule"

    # no frontmatter passes through unchanged
    plain = "# Plain rule\nDo this."
    _, out_content, _ = variant.transform("rule", plain, "style", "my-toolkit")
    assert out_content == plain


def test_cursor_transform_rule() -> None:
    variant = _CursorAgent()

    # adds alwaysApply, derives description from heading
    content = "---\nname: test\n---\n# My Rule\nContent here"
    out_type, out_content, out_name = variant.transform("rule", content, "coding", "my-toolkit")
    assert (out_type, out_name) == ("rule", "my-toolkit-coding.mdc")
    fm, _ = parse_frontmatter(out_content)
    assert fm["alwaysApply"] is True
    assert fm["description"] == "My Rule"

    # preserves existing description
    content2 = "---\ndescription: Custom desc\n---\n# Heading\nBody"
    _, out_content2, _ = variant.transform("rule", content2, "coding", "my-toolkit")
    fm2, _ = parse_frontmatter(out_content2)
    assert fm2["description"] == "Custom desc"
    assert fm2["alwaysApply"] is True

    # no heading means no description key
    _, out_content3, _ = variant.transform("rule", "Just text", "coding", "my-toolkit")
    fm3, _ = parse_frontmatter(out_content3)
    assert fm3["alwaysApply"] is True
    assert "description" not in fm3


def test_codex_transform_command() -> None:
    """Codex converts commands to skills, preserving or deriving frontmatter."""
    variant = _CodexAgent()

    # with frontmatter: preserves name/description
    content = "---\nname: Bootstrap\ndescription: Set up project\n---\n# Bootstrap\nDo stuff"
    out_type, out_content, out_name = variant.transform("command", content, "bootstrap", "p")
    assert (out_type, out_name) == ("skill", "bootstrap")
    fm, body = parse_frontmatter(out_content)
    assert fm["name"] == "Bootstrap"
    assert fm["description"] == "Set up project"
    assert "# Bootstrap" in body

    # without frontmatter: derives from source_name and heading
    content2 = "# Bootstrap\nDo stuff"
    _, out_content2, _ = variant.transform("command", content2, "bootstrap", "p")
    fm2, _ = parse_frontmatter(out_content2)
    assert fm2["name"] == "bootstrap"
    assert fm2["description"] == "Bootstrap"


def test_codex_transform_rule() -> None:
    """Codex converts rules to skills with toolkit-prefixed names."""
    variant = _CodexAgent()

    # derives name from toolkit+source_name, description from heading
    content = "# Coding Style\nFollow these rules."
    out_type, out_content, out_name = variant.transform("rule", content, "coding", "my-toolkit")
    assert (out_type, out_name) == ("skill", "my-toolkit-coding")
    fm, body = parse_frontmatter(out_content)
    assert fm["name"] == "my-toolkit-coding"
    assert fm["description"] == "Coding Style"

    # with frontmatter: preserves name/description from source
    content2 = "---\nname: Custom\ndescription: Custom desc\n---\n# Rule\nBody"
    _, out_content2, out_name2 = variant.transform("rule", content2, "style", "my-toolkit")
    assert out_name2 == "my-toolkit-style"
    fm2, _ = parse_frontmatter(out_content2)
    assert fm2["name"] == "Custom"
    assert fm2["description"] == "Custom desc"


@pytest.mark.parametrize(
    ("env_vars", "expected_cls"),
    [
        ({"CLAUDECODE": "1"}, _ClaudeAgent),
        ({"CODEX_CI": "1"}, _CodexAgent),
        ({"CURSOR_AGENT": "1"}, _CursorAgent),
        ({"CLAUDECODE": "1", "CODEX_CI": "1"}, _ClaudeAgent),
        ({"CODEX_CI": "1", "CURSOR_AGENT": "1"}, _CodexAgent),
    ],
    ids=[
        "claude-env",
        "codex-env",
        "cursor-env",
        "claude-beats-codex",
        "codex-beats-cursor",
    ],
)
def test_detect_runtime_env(
    env_vars: Dict[str, str],
    expected_cls: Type[_AIAgent],
    tmp_path: Path,
    environment: Any,
) -> None:
    environment.update(env_vars)
    result = _AIAgent.detect(tmp_path)
    assert isinstance(result, expected_cls)


@pytest.mark.parametrize(
    ("probe_paths", "expected_cls"),
    [
        ([".claude"], _ClaudeAgent),
        (["AGENTS.md"], _CodexAgent),
        ([".cursor"], _CursorAgent),
        ([".claude", ".cursor"], _ClaudeAgent),
        (["AGENTS.md", ".cursor"], _CodexAgent),
    ],
    ids=[
        "claude-dir",
        "codex-agents-md",
        "cursor-dir",
        "claude-beats-cursor",
        "codex-beats-cursor",
    ],
)
def test_detect_project_probes(
    probe_paths: List[str],
    expected_cls: Type[_AIAgent],
    tmp_path: Path,
    environment: Any,
) -> None:
    for p in probe_paths:
        target = tmp_path / p
        if "." in p and not p.startswith("."):
            target.touch()
        else:
            target.mkdir(exist_ok=True)
    result = _AIAgent.detect(tmp_path)
    assert isinstance(result, expected_cls)


def test_detect_global_fallback(
    tmp_path: Path, environment: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Falls back to global home probes when project root has no markers."""
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    (fake_home / ".cursor").mkdir()
    monkeypatch.setattr("dlt._workspace.cli.ai.agents.home_dir", lambda: fake_home)

    project = tmp_path / "project"
    project.mkdir()
    result = _AIAgent.detect(project)
    assert isinstance(result, _CursorAgent)


def test_detect_project_probes_before_global(
    tmp_path: Path, environment: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Project-local probe wins even when a higher-priority variant exists globally."""
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    (fake_home / ".claude").mkdir()
    monkeypatch.setattr("dlt._workspace.cli.ai.agents.home_dir", lambda: fake_home)

    project = tmp_path / "project"
    project.mkdir()
    (project / ".cursor").mkdir()
    result = _AIAgent.detect(project)
    assert isinstance(result, _CursorAgent)


def test_detect_returns_none(
    tmp_path: Path, environment: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("dlt._workspace.cli.ai.agents.home_dir", lambda: None)
    assert _AIAgent.detect(tmp_path) is None


def test_detect_no_home(tmp_path: Path, environment: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Project probes still work when home directory is unavailable."""
    monkeypatch.setattr("dlt._workspace.cli.ai.agents.home_dir", lambda: None)

    project = tmp_path / "project"
    project.mkdir()
    assert _AIAgent.detect(project) is None

    (project / ".cursor").mkdir()
    assert isinstance(_AIAgent.detect(project), _CursorAgent)


@pytest.mark.parametrize(
    ("variant_name", "expected_path"),
    [
        ("claude", ".mcp.json"),
        ("cursor", ".cursor/mcp.json"),
        ("codex", ".codex/config.toml"),
    ],
    ids=["claude", "cursor", "codex"],
)
def test_variant_mcp_config_path(variant_name: str, expected_path: str, tmp_path: Path) -> None:
    variant = AI_AGENTS[variant_name]()
    assert variant.mcp_config_path(tmp_path) == tmp_path / expected_path
