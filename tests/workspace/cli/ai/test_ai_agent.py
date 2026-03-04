from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Type

import pytest

from dlt._workspace.cli.ai.agents import (
    AI_AGENTS,
    _AIAgent,
    _ClaudeAgent,
    _CodexAgent,
    _CursorAgent,
)
from dlt._workspace.cli.formatters import parse_frontmatter

from tests.workspace.cli.ai.utils import _ensure_init_agents_template


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
    ("variant_cls", "ignore_file_name"),
    [
        (_ClaudeAgent, ".claudeignore"),
        (_CursorAgent, ".cursorignore"),
        (_CodexAgent, ".codexignore"),
    ],
    ids=["claude", "cursor", "codex"],
)
def test_install_actions_skill(variant_cls: Type[_AIAgent], ignore_file_name: str) -> None:
    """Skill install_actions returns a single copytree action for all agents."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    skill_src = Path("src_skill")
    skill_src.mkdir(exist_ok=True)

    variant = variant_cls()
    actions = variant.install_actions("skill", skill_src, "my-skill", "p", project)
    assert len(actions) == 1
    assert actions[0].kind == "skill"
    assert actions[0].op == "copytree"
    assert actions[0].source_name == "my-skill"
    assert actions[0].content_or_path == skill_src


@pytest.mark.parametrize(
    ("variant_cls", "ignore_file_name"),
    [
        (_ClaudeAgent, ".claudeignore"),
        (_CursorAgent, ".cursorignore"),
        (_CodexAgent, ".codexignore"),
    ],
    ids=["claude", "cursor", "codex"],
)
def test_install_actions_ignore(variant_cls: Type[_AIAgent], ignore_file_name: str) -> None:
    """Ignore install_actions returns a save action with agent-specific filename."""
    project = Path("project")
    project.mkdir(exist_ok=True)

    variant = variant_cls()
    actions = variant.install_actions("ignore", "*.secret", ".claudeignore", "p", project)
    assert len(actions) == 1
    assert actions[0].kind == "ignore"
    assert actions[0].op == "save"
    assert actions[0].dest_path == project / ignore_file_name
    assert actions[0].content_or_path == "*.secret"


@pytest.mark.parametrize(
    ("variant_cls", "expected_dest_suffix"),
    [
        (_ClaudeAgent, ".claude/commands/bootstrap.md"),
        (_CursorAgent, ".cursor/commands/bootstrap.md"),
    ],
    ids=["claude", "cursor"],
)
def test_install_actions_command(variant_cls: Type[_AIAgent], expected_dest_suffix: str) -> None:
    """Command install_actions returns a single save action for Claude/Cursor."""
    project = Path("project")
    project.mkdir(exist_ok=True)

    variant = variant_cls()
    actions = variant.install_actions("command", "# Do", "bootstrap", "p", project)
    assert len(actions) == 1
    assert actions[0].kind == "command"
    assert actions[0].op == "save"
    assert actions[0].dest_path == project / expected_dest_suffix
    assert actions[0].content_or_path == "# Do"


def test_claude_install_actions_rule() -> None:
    """Claude rule install_actions strips non-Claude frontmatter keys."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    variant = _ClaudeAgent()

    # strips non-Claude frontmatter keys, keeps name/description
    content = "---\nalwaysApply: true\ndescription: Cursor rule\nname: keep-me\n---\n# Rule"
    actions = variant.install_actions("rule", content, "coding", "my-toolkit", project)
    assert len(actions) == 1
    a = actions[0]
    assert a.kind == "rule"
    assert a.dest_path == project / ".claude/rules/my-toolkit-coding.md"
    fm, body = parse_frontmatter(a.content_or_path)  # type: ignore[arg-type]
    assert "alwaysApply" not in fm
    assert fm.get("name") == "keep-me"
    assert body == "# Rule"

    # no frontmatter passes through unchanged
    plain = "# Plain rule\nDo this."
    actions2 = variant.install_actions("rule", plain, "style", "my-toolkit", project)
    assert actions2[0].content_or_path == plain


def test_cursor_install_actions_rule() -> None:
    """Cursor rule install_actions adds alwaysApply and derives description."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    variant = _CursorAgent()

    # adds alwaysApply, derives description from heading
    content = "---\nname: test\n---\n# My Rule\nContent here"
    actions = variant.install_actions("rule", content, "coding", "my-toolkit", project)
    assert len(actions) == 1
    a = actions[0]
    assert a.kind == "rule"
    assert a.dest_path == project / ".cursor/rules/my-toolkit-coding.mdc"
    fm, _ = parse_frontmatter(a.content_or_path)  # type: ignore[arg-type]
    assert fm["alwaysApply"] is True
    assert fm["description"] == "My Rule"

    # preserves existing description
    content2 = "---\ndescription: Custom desc\n---\n# Heading\nBody"
    actions2 = variant.install_actions("rule", content2, "coding", "my-toolkit", project)
    fm2, _ = parse_frontmatter(actions2[0].content_or_path)  # type: ignore[arg-type]
    assert fm2["description"] == "Custom desc"
    assert fm2["alwaysApply"] is True

    # no heading means no description key
    actions3 = variant.install_actions("rule", "Just text", "coding", "my-toolkit", project)
    fm3, _ = parse_frontmatter(actions3[0].content_or_path)  # type: ignore[arg-type]
    assert fm3["alwaysApply"] is True
    assert "description" not in fm3


def test_codex_install_actions_command() -> None:
    """Codex converts commands to skills, preserving or deriving frontmatter."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    variant = _CodexAgent()

    # with frontmatter: preserves name/description
    content = "---\nname: Bootstrap\ndescription: Set up project\n---\n# Bootstrap\nDo stuff"
    actions = variant.install_actions("command", content, "bootstrap", "p", project)
    assert len(actions) == 1
    a = actions[0]
    assert a.kind == "skill"
    assert a.dest_path == project / ".agents/skills/bootstrap/SKILL.md"
    fm, body = parse_frontmatter(a.content_or_path)  # type: ignore[arg-type]
    assert fm["name"] == "Bootstrap"
    assert fm["description"] == "Set up project"
    assert "# Bootstrap" in body

    # without frontmatter: derives from source_name and heading
    content2 = "# Bootstrap\nDo stuff"
    actions2 = variant.install_actions("command", content2, "bootstrap", "p", project)
    fm2, _ = parse_frontmatter(actions2[0].content_or_path)  # type: ignore[arg-type]
    assert fm2["name"] == "bootstrap"
    assert fm2["description"] == "Bootstrap"


def test_codex_install_actions_rule() -> None:
    """Codex converts rules to always-apply skills with toolkit-prefixed names."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    variant = _CodexAgent()

    # derives name from toolkit+source_name, description from heading
    content = "# Coding Style\nFollow these rules."
    actions = variant.install_actions("rule", content, "coding", "my-toolkit", project)
    # install_actions returns just the skill; AGENTS.md is handled by finalize_actions
    assert len(actions) == 1
    skill_action = actions[0]
    assert skill_action.kind == "skill"
    assert skill_action.dest_path == project / ".agents/skills/my-toolkit-coding/SKILL.md"
    fm, _ = parse_frontmatter(skill_action.content_or_path)  # type: ignore[arg-type]
    assert fm["name"] == "my-toolkit-coding"
    assert fm["description"] == "ALWAYS read and follow this skill before acting. Coding Style"

    # with frontmatter: preserves name/description from source
    content2 = "---\nname: Custom\ndescription: Custom desc\n---\n# Rule\nBody"
    actions2 = variant.install_actions("rule", content2, "style", "my-toolkit", project)
    assert len(actions2) == 1
    fm2, _ = parse_frontmatter(actions2[0].content_or_path)  # type: ignore[arg-type]
    assert fm2["name"] == "Custom"
    assert fm2["description"] == "ALWAYS read and follow this skill before acting. Custom desc"


def test_codex_finalize_actions_agents_md() -> None:
    """finalize_actions produces a single AGENTS.md action from all always-apply skills."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    wb = Path("workbench")
    wb.mkdir(exist_ok=True)
    _ensure_init_agents_template(wb)
    variant = _CodexAgent()

    # simulate two rule→skill actions
    from dlt._workspace.cli.ai.agents import InstallAction

    skills_dir = variant.component_dir("skill", project)
    actions = [
        InstallAction(
            kind="skill",
            source_name="coding",
            dest_path=skills_dir / "tk-coding" / "SKILL.md",
            op="save",
            content_or_path=(
                "---\nname: tk-coding\ndescription: ALWAYS read and follow"
                " this skill before acting. Coding\n---\n# Coding"
            ),
            conflict=False,
            source_kind="rule",
        ),
        InstallAction(
            kind="skill",
            source_name="styling",
            dest_path=skills_dir / "tk-styling" / "SKILL.md",
            op="save",
            content_or_path=(
                "---\nname: tk-styling\ndescription: ALWAYS read and follow"
                " this skill before acting. Styling\n---\n# Styling"
            ),
            conflict=False,
            source_kind="rule",
        ),
    ]
    result = variant.finalize_actions(actions, project, workbench_base=wb)
    assert len(result) == 3
    agents_action = result[-1]
    assert agents_action.kind == "rule"
    assert agents_action.dest_path == project / "AGENTS.md"
    content = str(agents_action.content_or_path)
    assert "`tk-coding`" in content
    assert "`tk-styling`" in content
    assert "## ALWAYS ACTIVATE those skills" in content


def test_codex_finalize_actions_dedup() -> None:
    """finalize_actions skips AGENTS.md when all skills are already listed."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    agents_md = project / "AGENTS.md"
    agents_md.write_text("## ALWAYS ACTIVATE those skills\n- `tk-coding`\n", encoding="utf-8")
    wb = Path("workbench")
    wb.mkdir(exist_ok=True)
    _ensure_init_agents_template(wb)

    variant = _CodexAgent()
    from dlt._workspace.cli.ai.agents import InstallAction

    skills_dir = variant.component_dir("skill", project)
    actions = [
        InstallAction(
            kind="skill",
            source_name="coding",
            dest_path=skills_dir / "tk-coding" / "SKILL.md",
            op="save",
            content_or_path=(
                "---\nname: tk-coding\ndescription: ALWAYS read and follow"
                " this skill before acting. Coding\n---\n# Coding"
            ),
            conflict=False,
            source_kind="rule",
        ),
    ]
    result = variant.finalize_actions(actions, project, workbench_base=wb)
    # no AGENTS.md action added since skill is already listed
    assert len(result) == 1
    assert all(a.kind == "skill" for a in result)


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
