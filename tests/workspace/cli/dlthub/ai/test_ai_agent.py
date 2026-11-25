import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Type

import pytest
from packaging.requirements import Requirement

from dlt._workspace.cli.dlthub.ai.agents import (
    AI_AGENTS,
    _AIAgent,
    _ClaudeAgent,
    _CodexAgent,
    _CortexAgent,
    _CursorAgent,
)
from dlt._workspace.cli.markdown import MarkdownDocument


@pytest.fixture()
def no_home_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Suppress global home-directory detection so only env probes fire."""

    monkeypatch.setattr("dlt._workspace.cli.dlthub.ai.agents.home_dir", lambda: None)


@pytest.mark.parametrize(
    ("variant_name", "expected_skill", "expected_command", "expected_rule", "expected_agent"),
    [
        ("claude", ".claude/skills", ".claude/commands", ".claude/rules", ".claude/dlthub/agents"),
        ("cursor", ".cursor/skills", ".cursor/commands", ".cursor/rules", ".cursor/dlthub/agents"),
        # codex installs rules as skills, so both live in the skills folder
        ("codex", ".agents/skills", None, ".agents/skills", ".agents/dlthub/agents"),
        ("cortex", ".cortex/skills", ".cortex/commands", ".cortex/rules", ".cortex/dlthub/agents"),
    ],
    ids=["claude", "cursor", "codex", "cortex"],
)
def test_variant_component_dir(
    variant_name: str,
    expected_skill: str,
    expected_command: str,
    expected_rule: str,
    expected_agent: str,
) -> None:
    project = Path("project")
    project.mkdir(exist_ok=True)
    variant = AI_AGENTS[variant_name]()
    assert variant.component_dir("skill", project) == project / expected_skill
    # dlt agents sit apart from the host's native agents, which live in `<host>/agents`
    assert variant.component_dir("agent", project) == project / expected_agent

    if expected_command is not None:
        assert variant.component_dir("command", project) == project / expected_command
    else:
        with pytest.raises(AssertionError):
            variant.component_dir("command", project)

    if expected_rule is not None:
        assert variant.component_dir("rule", project) == project / expected_rule
    else:
        with pytest.raises(AssertionError):
            variant.component_dir("rule", project)

    # ignore always resolves to project root
    assert variant.component_dir("ignore", project) == project


@pytest.mark.parametrize(
    "variant_cls",
    [_ClaudeAgent, _CursorAgent, _CodexAgent, _CortexAgent],
    ids=["claude", "cursor", "codex", "cortex"],
)
def test_install_actions_skill(variant_cls: Type[_AIAgent]) -> None:
    """Skill install_actions copies each file of the skill folder, compiled files excluded."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    skill_src = Path("src_skill")
    (skill_src / "refs").mkdir(parents=True, exist_ok=True)
    (skill_src / "__pycache__").mkdir(exist_ok=True)
    (skill_src / "SKILL.md").write_text("---\nname: my-skill\n---\nbody", encoding="utf-8")
    (skill_src / "refs" / "guide.md").write_text("guide", encoding="utf-8")
    (skill_src / "__pycache__" / "helper.cpython-312.pyc").write_bytes(b"")

    variant = variant_cls()
    actions = variant.install_actions("skill", skill_src, "my-skill", "p", project)
    skill_dest = variant.component_dir("skill", project) / "my-skill"
    assert {a.dest_path for a in actions} == {
        skill_dest / "SKILL.md",
        skill_dest / "refs" / "guide.md",
    }
    for action in actions:
        assert action.kind == "skill"
        assert action.op == "copy"
        assert action.source_name == "my-skill"
        assert action.content_or_path == skill_src / action.dest_path.relative_to(skill_dest)


@pytest.mark.parametrize(
    ("variant_cls", "ignore_file_name"),
    [
        (_ClaudeAgent, ".claudeignore"),
        (_CursorAgent, ".cursorignore"),
        (_CodexAgent, ".codexignore"),
        (_CortexAgent, None),
    ],
    ids=["claude", "cursor", "codex", "cortex-no-ignore-file"],
)
def test_install_actions_ignore(
    variant_cls: Type[_AIAgent], ignore_file_name: Optional[str]
) -> None:
    """Ignore install_actions returns a save action with agent-specific filename."""
    project = Path("project")
    project.mkdir(exist_ok=True)

    variant = variant_cls()
    actions = variant.install_actions("ignore", "*.secret", ".claudeignore", "p", project)
    if ignore_file_name is None:
        assert actions == []
        return
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
    fm, body = MarkdownDocument(str(a.content_or_path)).split_frontmatter()
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
    fm, _ = MarkdownDocument(str(a.content_or_path)).split_frontmatter()
    assert fm["alwaysApply"] is True
    assert fm["description"] == "My Rule"

    # preserves existing description
    content2 = "---\ndescription: Custom desc\n---\n# Heading\nBody"
    actions2 = variant.install_actions("rule", content2, "coding", "my-toolkit", project)
    fm2, _ = MarkdownDocument(str(actions2[0].content_or_path)).split_frontmatter()
    assert fm2["description"] == "Custom desc"
    assert fm2["alwaysApply"] is True

    # no heading means no description key
    actions3 = variant.install_actions("rule", "Just text", "coding", "my-toolkit", project)
    fm3, _ = MarkdownDocument(str(actions3[0].content_or_path)).split_frontmatter()
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
    fm, body = MarkdownDocument(str(a.content_or_path)).split_frontmatter()
    assert fm["name"] == "Bootstrap"
    assert fm["description"] == "Set up project"
    assert "# Bootstrap" in body

    # without frontmatter: derives from source_name and heading
    content2 = "# Bootstrap\nDo stuff"
    actions2 = variant.install_actions("command", content2, "bootstrap", "p", project)
    fm2, _ = MarkdownDocument(str(actions2[0].content_or_path)).split_frontmatter()
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
    # install_actions returns only the skill. The session start hook lists it
    assert len(actions) == 1
    skill_action = actions[0]
    assert skill_action.kind == "skill"
    assert skill_action.dest_path == project / ".agents/skills/my-toolkit-coding/SKILL.md"
    fm, _ = MarkdownDocument(str(skill_action.content_or_path)).split_frontmatter()
    assert fm["name"] == "my-toolkit-coding"
    assert fm["description"] == "ALWAYS read and follow this skill before acting. Coding Style"

    # with frontmatter: preserves name/description from source
    content2 = "---\nname: Custom\ndescription: Custom desc\n---\n# Rule\nBody"
    actions2 = variant.install_actions("rule", content2, "style", "my-toolkit", project)
    assert len(actions2) == 1
    fm2, _ = MarkdownDocument(str(actions2[0].content_or_path)).split_frontmatter()
    assert fm2["name"] == "Custom"
    assert fm2["description"] == "ALWAYS read and follow this skill before acting. Custom desc"


def _rule_install_actions(variant: _AIAgent, project: Path) -> List[Any]:
    return variant.install_actions("rule", "# r\nrule", "r", "tk", project)


def _install_rules(variant: _AIAgent, project: Path, toolkit: str, rules: List[str]) -> None:
    for rule in rules:
        for action in variant.install_actions(
            "rule", "# %s\nFollow %s." % (rule, rule), rule, toolkit, project
        ):
            action.dest_path.parent.mkdir(parents=True, exist_ok=True)
            action.dest_path.write_text(str(action.content_or_path), encoding="utf-8")


def _render(variant: _AIAgent, project: Path) -> Optional[str]:
    """Runs the single render action of the agent as an install does and returns what it wrote."""
    actions = variant.get_post_finalize_actions([], project)
    assert [(a.op, a.kind, a.skip_index) for a in actions] == [("render", "rule", True)]
    assert actions[0].render is not None
    content = actions[0].render()
    if content is not None:
        actions[0].dest_path.write_text(content, encoding="utf-8")
    return content


def test_codex_session_context() -> None:
    """Codex gets links to the skills that rules are installed as when a session starts."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    variant = _CodexAgent()
    assert variant.get_session_context(project) is None

    _install_rules(variant, project, "tk", ["styling", "coding"])
    # an ordinary skill is not a rule, and a skill the user broke is skipped
    skill = project / ".agents/skills/setup/SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text("---\nname: setup\ndescription: Set up things\n---\n# Setup", "utf-8")
    broken = project / ".agents/skills/broken/SKILL.md"
    broken.parent.mkdir(parents=True)
    broken.write_text("---\ndescription: [unclosed\n---\n# Broken", "utf-8")

    # rules are found through the rule dir, which codex shares with skills
    assert variant.component_dir("rule", project) == project / ".agents/skills"
    assert variant.get_session_context(project) == (
        _CodexAgent.RULES_INTRO
        + "\n- `tk-coding`: .agents/skills/tk-coding/SKILL.md"
        + "\n- `tk-styling`: .agents/skills/tk-styling/SKILL.md"
    )

    shutil.rmtree(project / ".agents/skills/tk-styling")
    assert "tk-styling" not in variant.get_session_context(project)


def test_codex_finalize_actions_adds_rules_hook() -> None:
    """A toolkit with rules adds the session start hook once and keeps the user's hooks."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    variant = _CodexAgent()
    hooks_json = project / ".codex/hooks.json"

    # no rules, no hook
    skill_actions = variant.install_actions("command", "# c\ncmd", "c", "tk", project)
    assert variant.finalize_actions(list(skill_actions), project) == skill_actions

    user_hook = {"matcher": "startup", "hooks": [{"type": "command", "command": "echo hi"}]}
    hooks_json.parent.mkdir(parents=True)
    hooks_json.write_text(
        json.dumps({"hooks": {"SessionStart": [user_hook]}, "other": 1}), encoding="utf-8"
    )
    actions = variant.finalize_actions(_rule_install_actions(variant, project), project)
    hook = actions[-1]
    assert (hook.kind, hook.source_name, hook.op, hook.dest_path, hook.skip_index) == (
        "hook",
        "SessionStart",
        "save",
        hooks_json,
        True,
    )
    merged = json.loads(str(hook.content_or_path))
    assert merged["other"] == 1
    assert merged["hooks"]["SessionStart"] == [user_hook, _CodexAgent.RULES_HOOK]

    # the hook is already there: no action
    hooks_json.write_text(str(hook.content_or_path), encoding="utf-8")
    rule_actions = _rule_install_actions(variant, project)
    assert variant.finalize_actions(list(rule_actions), project) == rule_actions


def test_cortex_render_cortex_md() -> None:
    """dlt owns the `# dltHub rules` section of CORTEX.md and rewrites it from the rule files."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    variant = _CortexAgent()
    cortex_md = project / "CORTEX.md"
    cortex_md.write_text("# My notes\nkeep me\n", encoding="utf-8")
    # rules keep their files, because background agents resolve rule references to them
    _install_rules(variant, project, "tk", ["coding", "styling"])
    _install_rules(variant, project, "other", ["x"])
    assert (project / ".cortex/rules/tk-coding.md").is_file()

    content = _render(variant, project)
    assert content is not None and cortex_md.read_text(encoding="utf-8") == content
    assert content.startswith("# My notes\nkeep me\n\n" + _CortexAgent.RULES_HEADING + "\n")
    # rule headings are demoted under the section heading, files in sorted order
    assert (
        content.index("## x\nFollow x.")
        < content.index("## coding\nFollow coding.")
        < content.index("## styling\nFollow styling.")
    )
    assert _render(variant, project) is None

    # text the user adds after the section, under its own top-level heading, is kept
    cortex_md.write_text(cortex_md.read_text(encoding="utf-8") + "\n# Later\nmine\n", "utf-8")
    # a deleted rule file drops out
    (project / ".cortex/rules/tk-styling.md").unlink()
    content = _render(variant, project)
    assert content is not None
    assert "Follow coding." in content
    assert "Follow styling." not in content
    assert content.rstrip().endswith("# Later\nmine")
    assert content.count(_CortexAgent.RULES_HEADING) == 1

    # a byte order mark that an editor saved does not hide the section
    cortex_md.write_text("﻿" + content.replace("Follow coding.", "edited"), "utf-8")
    content = _render(variant, project)
    assert content is not None and not content.startswith("﻿")
    assert content.count(_CortexAgent.RULES_HEADING) == 1
    assert "Follow coding." in content and "edited" not in content


def test_cortex_render_keeps_section_bounds() -> None:
    """Rule bodies with setext and nested headings or an unclosed fence stay in the section."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    variant = _CortexAgent()
    rules_dir = project / ".cortex/rules"
    rules_dir.mkdir(parents=True)
    (rules_dir / "tk-a.md").write_text("Title\n=====\n\n> # quoted\n\n- # listed\n", "utf-8")
    (rules_dir / "tk-b.md").write_text("# Code\n```py\nprint(1)\n", "utf-8")
    # a rule with broken frontmatter or bytes that are not UTF-8 is still rendered
    (rules_dir / "tk-c.md").write_bytes(b"---\n: broken [\n---\n# Bytes \xff\n")
    (rules_dir / "tk-d.md").write_text("﻿---\ndescription: bom\n---\n# Bom\n", "utf-8")
    cortex_md = project / "CORTEX.md"
    cortex_md.write_text("# Mine\nkeep\n", encoding="utf-8")

    first = _render(variant, project)
    assert first is not None
    assert "## Title\n" in first and "> # quoted" in first
    assert "print(1)\n```" in first
    assert "## Bytes �" in first and ": broken [" not in first
    assert "## Bom" in first and "description: bom" not in first
    # user text appended after the section survives every render
    cortex_md.write_text(first + "\n# After\nmine\n", encoding="utf-8")
    for _ in range(3):
        assert _render(variant, project) is None
    assert cortex_md.read_text(encoding="utf-8").endswith("# After\nmine\n")


def test_agents_without_rendered_files() -> None:
    """Only Cortex renders a file from the installed rules."""
    project = Path("project")
    project.mkdir(exist_ok=True)
    for variant in (_ClaudeAgent(), _CursorAgent(), _CodexAgent()):
        _install_rules(variant, project, "tk", ["coding"])
        assert variant.get_post_finalize_actions([], project) == []
    for variant in (_ClaudeAgent(), _CursorAgent()):
        actions = _rule_install_actions(variant, project)
        assert variant.finalize_actions(list(actions), project) == actions
        assert variant.get_session_context(project) is None


def test_cortex_merge_mcp_servers_adds_type() -> None:
    """The Cortex Code CLI requires `type`: servers started by a command get `stdio`."""
    merged = _CortexAgent().merge_mcp_servers(
        "",
        {
            "dlt": {"command": "uv", "args": ["run", "dlthub", "ai", "mcp", "--stdio"]},
            "remote": {"type": "http", "url": "https://example.com/mcp"},
        },
    )
    servers = _CortexAgent().parse_mcp_servers(merged)
    assert servers["dlt"]["type"] == "stdio"
    assert servers["remote"] == {"type": "http", "url": "https://example.com/mcp"}


def test_cortex_workspace_python_dependencies() -> None:
    """A cortex workspace gets no dlthub-client and no marimo. It gets dlthub directly."""
    from dlt._workspace.cli.dlthub.utils import workspace_deps
    from dlt._workspace.deployment.requirements import get_workspace_install_specs

    specs, deps = _CortexAgent().filter_workspace_python_dependencies(
        get_workspace_install_specs(), workspace_deps()
    )
    assert "dlthub-client" not in {s["name"] for s in specs}
    dlt_spec = next(s for s in specs if s["name"] == "dlt")
    assert "hub" not in dlt_spec["extras"]
    names = [Requirement(d).name for d in deps]
    assert "marimo" not in names
    assert "mowidgets" not in names
    assert "dlthub" in names or "dlthub" in {s["name"] for s in specs}
    # other agents keep the dlthub defaults
    assert _ClaudeAgent().filter_workspace_python_dependencies(specs, deps) == (specs, deps)


@pytest.mark.parametrize(
    ("env_vars", "expected_names"),
    [
        ({"CLAUDECODE": "1"}, {"claude"}),
        ({"CODEX_CI": "1"}, {"codex"}),
        ({"CURSOR_AGENT": "1"}, {"cursor"}),
        ({"CORTEX_CODE_CLIENT_SURFACE": "coco_cli"}, {"cortex"}),
        # an agent started from the shell of another agent inherits its environment variables,
        # so both are detected
        ({"CORTEX_CODE_CLIENT_SURFACE": "coco_cli", "CLAUDECODE": "1"}, {"cortex", "claude"}),
    ],
    ids=["claude-env", "codex-env", "cursor-env", "cortex-env", "cortex-in-claude-env"],
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
        (["CORTEX.md"], [".snowflake/cortex"], {"cortex"}),
        # LOCAL probe without matching global marker → not detected at LOCAL level
        ([".cursor"], [], set()),
        (["AGENTS.md"], [], set()),
        # a Snowflake CLI config alone does not mean Cortex Code is installed
        ([".cortex"], [".snowflake"], set()),
    ],
    ids=[
        "claude-dir",
        "codex-agents-md",
        "cursor-dir",
        "claude-and-cursor",
        "codex-and-cursor",
        "cortex-md",
        "cursor-no-global",
        "agents-md-no-global",
        "snowflake-cli-no-cortex",
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
        (fake_home / m).mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("dlt._workspace.cli.dlthub.ai.agents.home_dir", lambda: fake_home)
    # the Cortex marker is then looked up in the fake home
    monkeypatch.delenv("SNOWFLAKE_HOME")

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
    monkeypatch.setattr("dlt._workspace.cli.dlthub.ai.agents.home_dir", lambda: fake_home)

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
    monkeypatch.setattr("dlt._workspace.cli.dlthub.ai.agents.home_dir", lambda: fake_home)

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
    monkeypatch.setattr("dlt._workspace.cli.dlthub.ai.agents.home_dir", lambda: fake_home)

    project = Path("project")
    project.mkdir()
    # cursor detected at LOCAL (project .cursor + global .cursor)
    # claude detected at GLOBAL only (no project marker)
    (project / ".cursor").mkdir()
    agents = _AIAgent.detect_all(project)
    assert [a.name for a, _ in agents] == ["cursor", "claude"]


def test_detect_best_agents(environment: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Only agents at the strongest level count. Several of them leave the agent unknown."""
    fake_home = Path("home")
    fake_home.mkdir()
    (fake_home / ".claude").mkdir()
    (fake_home / ".cursor").mkdir()
    monkeypatch.setattr("dlt._workspace.cli.dlthub.ai.agents.home_dir", lambda: fake_home)
    project = Path("project")
    project.mkdir()

    assert {a.name for a in _AIAgent.detect_best_agents(project)} == {"claude", "cursor"}
    assert _AIAgent.detect_agent_name(None, project) is None
    # an explicit agent always wins
    assert _AIAgent.detect_agent_name("codex", project) == "codex"

    (project / ".cursor").mkdir()
    assert [a.name for a in _AIAgent.detect_best_agents(project)] == ["cursor"]
    assert _AIAgent.detect_agent_name(None, project) == "cursor"

    environment["CLAUDECODE"] = "1"
    assert [a.name for a in _AIAgent.detect_best_agents(project)] == ["claude"]


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
    ("variant_name", "expected_paths"),
    [
        ("claude", [".mcp.json"]),
        ("cursor", [".cursor/mcp.json"]),
        ("codex", [".codex/config.toml"]),
        # Desktop reads the project config, the CLI only the user one under SNOWFLAKE_HOME
        ("cortex", [".cortex/mcp.json", "<snowflake_home>/cortex/mcp.json"]),
    ],
    ids=["claude", "cursor", "codex", "cortex"],
)
def test_variant_get_mcp_config_paths(
    variant_name: str, expected_paths: List[str], _snowflake_home: Path
) -> None:
    project = Path("project")
    project.mkdir(exist_ok=True)
    variant = AI_AGENTS[variant_name]()
    expected = [
        (_snowflake_home / p.split("/", 1)[1] if p.startswith("<snowflake_home>") else project / p)
        for p in expected_paths
    ]
    assert variant.get_mcp_config_paths(project) == expected
