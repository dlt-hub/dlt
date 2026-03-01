from os import environ
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Type

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


@pytest.fixture()
def no_home_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Suppress global home-directory detection so only env probes fire.

    Note: LOCAL detection also requires the tool's global marker to exist,
    so with no home dir only ENV detection works.
    """
    monkeypatch.setattr("dlt._workspace.cli.ai.agents.home_dir", lambda: None)


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
) -> None:
    project = Path("project")
    project.mkdir(exist_ok=True)
    variant = AI_AGENTS[variant_name]()
    assert variant.component_dir("skill", project) == project / expected_skill

    if expected_command is not None:
        assert variant.component_dir("command", project) == project / expected_command
    else:
        with pytest.raises(NotImplementedError):
            variant.component_dir("command", project)

    if expected_rule is not None:
        assert variant.component_dir("rule", project) == project / expected_rule
    else:
        with pytest.raises(NotImplementedError):
            variant.component_dir("rule", project)

    # ignore always resolves to project root
    assert variant.component_dir("ignore", project) == project


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
    assert fm["description"] == "ALWAYS read and follow this skill before acting. Coding Style"

    # with frontmatter: preserves name/description from source
    content2 = "---\nname: Custom\ndescription: Custom desc\n---\n# Rule\nBody"
    _, out_content2, out_name2 = variant.transform("rule", content2, "style", "my-toolkit")
    assert out_name2 == "my-toolkit-style"
    fm2, _ = parse_frontmatter(out_content2)
    assert fm2["name"] == "Custom"
    assert fm2["description"] == "ALWAYS read and follow this skill before acting. Custom desc"


@pytest.mark.parametrize(
    ("env_vars", "expected_names"),
    [
        ({"CLAUDECODE": "1"}, {"claude"}),
        ({"CODEX_CI": "1"}, {"codex"}),
        ({"CURSOR_AGENT": "1"}, {"cursor"}),
        ({"CLAUDECODE": "1", "CODEX_CI": "1"}, {"claude", "codex"}),
        ({"CODEX_CI": "1", "CURSOR_AGENT": "1"}, {"codex", "cursor"}),
        ({"CLAUDECODE": "1", "CODEX_CI": "1", "CURSOR_AGENT": "1"}, {"claude", "codex", "cursor"}),
    ],
    ids=[
        "claude-env",
        "codex-env",
        "cursor-env",
        "claude-and-codex",
        "codex-and-cursor",
        "all-three",
    ],
)
def test_detect_all_runtime_env(
    env_vars: Dict[str, str],
    expected_names: Set[str],
    environment: Any,
    no_home_dir: None,
) -> None:
    environment.update(env_vars)
    project = Path("project")
    project.mkdir(exist_ok=True)
    agents = _AIAgent.detect_all(project)
    assert {a.name for a, _ in agents} == expected_names


@pytest.mark.parametrize(
    ("probe_paths", "global_markers", "expected_names"),
    [
        ([".claude"], [".claude"], {"claude"}),
        (["AGENTS.md"], [".codex"], {"codex"}),
        ([".cursor"], [".cursor"], {"cursor"}),
        ([".claude", ".cursor"], [".claude", ".cursor"], {"claude", "cursor"}),
        (["AGENTS.md", ".cursor"], [".codex", ".cursor"], {"codex", "cursor"}),
        # LOCAL probe without matching global marker → not detected at LOCAL level
        ([".cursor"], [], set()),
        (["AGENTS.md"], [], set()),
    ],
    ids=[
        "claude-dir",
        "codex-agents-md",
        "cursor-dir",
        "claude-and-cursor",
        "codex-and-cursor",
        "cursor-no-global",
        "agents-md-no-global",
    ],
)
def test_detect_all_project_probes(
    probe_paths: List[str],
    global_markers: List[str],
    expected_names: Set[str],
    environment: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LOCAL detection requires the tool's global marker (~/.tool) to exist."""
    fake_home = Path("home")
    fake_home.mkdir()
    for m in global_markers:
        (fake_home / m).mkdir(exist_ok=True)
    monkeypatch.setattr("dlt._workspace.cli.ai.agents.home_dir", lambda: fake_home)

    project = Path("project")
    project.mkdir()
    for p in probe_paths:
        target = project / p
        if "." in p and not p.startswith("."):
            target.touch()
        else:
            target.mkdir(exist_ok=True)
    agents = _AIAgent.detect_all(project)
    assert {a.name for a, _ in agents} == expected_names


def test_detect_all_global_fallback(environment: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Falls back to global home probes when project root has no markers."""
    fake_home = Path("home")
    fake_home.mkdir()
    (fake_home / ".cursor").mkdir()
    monkeypatch.setattr("dlt._workspace.cli.ai.agents.home_dir", lambda: fake_home)

    project = Path("project")
    project.mkdir()
    agents = _AIAgent.detect_all(project)
    assert [a.name for a, _ in agents] == ["cursor"]


def test_detect_all_env_before_local(
    environment: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ENV-detected agent comes before LOCAL-detected agent."""
    fake_home = Path("home")
    fake_home.mkdir()
    (fake_home / ".claude").mkdir()
    monkeypatch.setattr("dlt._workspace.cli.ai.agents.home_dir", lambda: fake_home)

    environment["CURSOR_AGENT"] = "1"
    project = Path("project")
    project.mkdir()
    (project / ".claude").mkdir()
    agents = _AIAgent.detect_all(project)
    assert [a.name for a, _ in agents] == ["cursor", "claude"]


def test_detect_all_local_before_global(environment: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Project-local probe comes before global-only probe in the list."""
    fake_home = Path("home")
    fake_home.mkdir()
    (fake_home / ".claude").mkdir()
    (fake_home / ".cursor").mkdir()
    monkeypatch.setattr("dlt._workspace.cli.ai.agents.home_dir", lambda: fake_home)

    project = Path("project")
    project.mkdir()
    # cursor detected at LOCAL (project .cursor + global .cursor)
    # claude detected at GLOBAL only (no project marker)
    (project / ".cursor").mkdir()
    agents = _AIAgent.detect_all(project)
    assert [a.name for a, _ in agents] == ["cursor", "claude"]


def test_detect_all_returns_empty(environment: Any, no_home_dir: None) -> None:
    empty = Path("empty_dir")
    empty.mkdir()
    assert _AIAgent.detect_all(empty) == []


def test_detect_all_no_home(environment: Any, no_home_dir: None) -> None:
    """No detection without home directory (LOCAL needs global marker too)."""
    project = Path("project")
    project.mkdir()
    assert _AIAgent.detect_all(project) == []

    # LOCAL probes alone are not enough without the global marker
    (project / ".cursor").mkdir()
    assert _AIAgent.detect_all(project) == []


@pytest.mark.parametrize(
    ("variant_name", "expected_path"),
    [
        ("claude", ".mcp.json"),
        ("cursor", ".cursor/mcp.json"),
        ("codex", ".codex/config.toml"),
    ],
    ids=["claude", "cursor", "codex"],
)
def test_variant_mcp_config_path(variant_name: str, expected_path: str) -> None:
    project = Path("project")
    project.mkdir(exist_ok=True)
    variant = AI_AGENTS[variant_name]()
    assert variant.mcp_config_path(project) == project / expected_path
