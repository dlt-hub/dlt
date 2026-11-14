import functools
import os
from abc import ABC, abstractmethod
from enum import IntEnum
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from packaging.requirements import Requirement

from dlt.common.runtime.exec_info import is_claude_code, is_codex, is_cortex, is_cursor

from dlt._workspace.cli.dlthub.ai.typing import TAIAgentName
from dlt._workspace.cli.dlthub.ai.utils import (
    DLTHUB_AGENTS_DIR,
    cap_skill_description,
    ensure_cursor_rule_frontmatter,
    home_dir,
    is_always_apply_skill,
    load_toolkits_index,
    merge_json_hooks,
    merge_json_mcp_servers,
    merge_toml_mcp_servers,
    parse_json_mcp,
    parse_toml_mcp,
    read_rule_body,
    strip_rule_frontmatter,
    wrap_as_skill,
    _INIT_TOOLKIT,
)
from dlt._workspace.cli.markdown import MarkdownDocument

if TYPE_CHECKING:
    # the deployment package imports this module (agent manifests), so it is imported lazily
    from dlt._workspace.deployment.typing import TInstallSpec

TComponentType = Literal["skill", "command", "rule", "agent", "ignore", "mcp", "hook"]
TInstallOp = Literal["copy", "save", "remove", "render"]

COMPONENT_MARKERS: Dict[TComponentType, str] = {
    "skill": "SKILL.md",
    "agent": "AGENT.md",
}
"""File that must exist inside a directory-based component for it to count as one."""


class AgentDetectLevel(IntEnum):
    ENV = 0
    LOCAL = 1
    GLOBAL = 2


class InstallAction(NamedTuple):
    kind: TComponentType
    source_name: str
    dest_path: Path
    op: TInstallOp
    content_or_path: Union[str, Path]
    conflict: bool
    source_kind: Optional[TComponentType] = None
    skip_index: bool = False
    component_dir: Optional[Path] = None
    """For `remove`: directory of the component; empty folders are pruned only below it."""
    render: Optional[Callable[[], Optional[str]]] = None
    """For `render`: new content of `dest_path`, or `None` when it stays as is. Runs after all
    other actions, so it sees the files they wrote."""
    mcp_servers: Tuple[str, ...] = ()
    """For `mcp`: servers the toolkit provides, recorded in the toolkit index."""


class _AIAgent(ABC):
    """AI coding agent adapter for toolkit installation, detection, and MCP configuration."""

    @property
    @abstractmethod
    def name(self) -> TAIAgentName: ...

    _DIRS: ClassVar[Dict[TComponentType, str]]
    """maps component types to relative directory paths under project root"""

    _GLOBAL_MARKER: ClassVar[str]
    """home-dir marker that proves the tool is installed (e.g. ".claude")"""

    _LOCAL_PROBES: ClassVar[Tuple[str, ...]]
    """project-root paths that indicate usage of this agent"""

    _RULE_EXT: ClassVar[str] = ".md"
    """file extension for rule files"""

    _IGNORE_FILE: ClassVar[Optional[str]] = None
    """name of the agent's ignore file, `None` when the agent has none"""

    @property
    def ignore_file_name(self) -> Optional[str]:
        """The agent's ignore file name, `None` when it has none."""
        return self._IGNORE_FILE

    def component_dir(self, component_type: TComponentType, project_root: Path) -> Path:
        """Root directory for the given component type."""
        if component_type == "ignore":
            return project_root
        assert component_type in self._DIRS, "%s not in _DIRS for %s" % (component_type, self.name)
        return project_root / self._DIRS[component_type]

    def install_actions(
        self,
        component_type: TComponentType,
        content_or_path: Union[str, Path],
        source_name: str,
        toolkit_name: str,
        project_root: Path,
        overwrite: bool = False,
    ) -> List["InstallAction"]:
        """Build install actions for a single component.

        Handles all component types: skill and agent (copy per file), ignore (save), and
        delegates command/rule to _install_command_or_rule().

        Args:
            component_type: "skill", "command", "rule", or "ignore"
            content_or_path: file content (str) or source directory (Path)
            source_name: original name (stem or dir name)
            toolkit_name: toolkit identifier
            project_root: target project root
            overwrite: if True, ignore existing conflicts

        Returns:
            list of InstallAction to execute
        """
        if component_type == "skill":
            assert isinstance(content_or_path, Path)
            return self._install_skill(content_or_path, source_name, project_root, overwrite)
        if component_type == "agent":
            assert isinstance(content_or_path, Path)
            return self._install_agent(
                content_or_path, source_name, toolkit_name, project_root, overwrite
            )
        if component_type == "ignore":
            assert isinstance(content_or_path, str)
            if self.ignore_file_name is None:
                return []
            dest = self.component_dir("ignore", project_root) / self.ignore_file_name
            return [
                InstallAction(
                    kind="ignore",
                    source_name=self.ignore_file_name,
                    dest_path=dest,
                    op="save",
                    content_or_path=content_or_path,
                    conflict=not overwrite and dest.exists(),
                )
            ]
        assert isinstance(content_or_path, str)
        return self._install_command_or_rule(
            component_type, content_or_path, source_name, toolkit_name, project_root, overwrite
        )

    def _install_skill(
        self,
        content_or_path: Path,
        source_name: str,
        project_root: Path,
        overwrite: bool,
    ) -> List["InstallAction"]:
        """Install actions for a skill directory, copied verbatim."""
        dest = self.component_dir("skill", project_root) / source_name
        return self._copy_folder_actions(
            "skill", source_name, content_or_path, dest, not overwrite and dest.exists()
        )

    def _install_agent(
        self,
        content_or_path: Path,
        source_name: str,
        toolkit_name: str,
        project_root: Path,
        overwrite: bool,
    ) -> List["InstallAction"]:
        """Install actions for an agent directory, copied verbatim under its toolkit's folder."""
        dest = self.component_dir("agent", project_root) / toolkit_name / source_name
        return self._copy_folder_actions(
            "agent", source_name, content_or_path, dest, not overwrite and dest.exists()
        )

    @staticmethod
    def _copy_folder_actions(
        kind: TComponentType, source_name: str, src_dir: Path, dest_dir: Path, conflict: bool
    ) -> List[InstallAction]:
        """One `copy` action per file of `src_dir`, all sharing the folder-level conflict."""
        actions: List[InstallAction] = []
        for src_file in sorted(src_dir.rglob("*")):
            # an agent may ship Python code; what a toolkit checkout compiled is not part of it
            if (
                not src_file.is_file()
                or "__pycache__" in src_file.parts
                or src_file.suffix == ".pyc"
            ):
                continue
            actions.append(
                InstallAction(
                    kind=kind,
                    source_name=source_name,
                    dest_path=dest_dir / src_file.relative_to(src_dir),
                    op="copy",
                    content_or_path=src_file,
                    conflict=conflict,
                )
            )
        return actions

    def component_path(
        self,
        component_type: TComponentType,
        source_name: str,
        toolkit_name: str,
        project_root: Path,
    ) -> Optional[Path]:
        """Path this agent installs a component to, or `None` when it has no place for the type."""
        if component_type not in self._DIRS:
            return None
        base = self.component_dir(component_type, project_root)
        marker = COMPONENT_MARKERS.get(component_type)
        # agents sit under their toolkit, so two toolkits may ship the same agent name
        if component_type == "agent":
            return base / toolkit_name / source_name / COMPONENT_MARKERS["agent"]
        if marker:
            return base / source_name / marker
        if component_type == "command":
            return base / (source_name + ".md")
        # rules are prefixed with the toolkit name
        return base / (toolkit_name + "-" + source_name + self._RULE_EXT)

    def _transform_rule(self, content: str) -> str:
        """Transform rule content before writing. Override for agent-specific formatting."""
        return content

    def _install_command_or_rule(
        self,
        component_type: TComponentType,
        content: str,
        source_name: str,
        toolkit_name: str,
        project_root: Path,
        overwrite: bool,
    ) -> List["InstallAction"]:
        """Install actions for commands and rules.

        Commands are saved as `source_name.md` under the command dir.
        Rules are transformed via `_transform_rule` and saved as
        `toolkit_name-source_name{_RULE_EXT}` under the rule dir.
        """
        if component_type == "command":
            dest = self.component_dir("command", project_root) / (source_name + ".md")
            return [
                InstallAction(
                    kind="command",
                    source_name=source_name,
                    dest_path=dest,
                    op="save",
                    content_or_path=content,
                    conflict=not overwrite and dest.exists(),
                )
            ]
        transformed = self._transform_rule(content)
        out_filename = toolkit_name + "-" + source_name + self._RULE_EXT
        dest = self.component_dir("rule", project_root) / out_filename
        return [
            InstallAction(
                kind="rule",
                source_name=source_name,
                dest_path=dest,
                op="save",
                content_or_path=transformed,
                conflict=not overwrite and dest.exists(),
            )
        ]

    def finalize_actions(
        self, actions: List["InstallAction"], project_root: Path
    ) -> List["InstallAction"]:
        """Post-process the full action list. Default is identity."""
        return actions

    def post_finalize_actions(
        self, actions: List["InstallAction"], project_root: Path
    ) -> List["InstallAction"]:
        """Actions that run after all other actions, e.g. to render a file from installed ones."""
        return []

    def installed_rule_files(self, project_root: Path) -> List[Path]:
        """Rule files installed in the project, sorted so the same rules give the same output."""
        if "rule" not in self._DIRS:
            return []
        return sorted(self.component_dir("rule", project_root).glob("*" + self._RULE_EXT))

    def session_context(self, project_root: Path) -> Optional[str]:
        """Text the agent gets when a session starts, `None` when it needs none."""
        return None

    def install_notes(self) -> List[str]:
        """Steps the user has to take after an install, e.g. to enable MCP servers."""
        return []

    def workspace_python_dependencies(
        self, specs: List["TInstallSpec"], deps: List[str]
    ) -> Tuple[List["TInstallSpec"], List[str]]:
        """Packages `dlthub init` writes for this agent: dlt package specs and other deps."""
        return specs, deps

    @abstractmethod
    def mcp_config_paths(self, project_root: Path) -> List[Path]:
        """MCP config files the agent reads, all in the same format."""

    @abstractmethod
    def parse_mcp_servers(self, content: str) -> Dict[str, Any]:
        """Parse existing MCP config content into {server_name: config}."""

    @abstractmethod
    def merge_mcp_servers(self, existing_content: str, new_servers: Dict[str, Any]) -> str:
        """Merge new server entries into existing config content."""

    @classmethod
    @abstractmethod
    def _is_env(cls) -> bool:
        """Return True when the agent's runtime env var is set."""

    @classmethod
    def _global_marker_path(cls) -> Optional[Path]:
        """Path that proves the tool is installed for the user."""
        home = home_dir()
        return home / cls._GLOBAL_MARKER if home is not None else None

    @classmethod
    def _detect(cls, project_root: Path) -> Optional[AgentDetectLevel]:
        """Detect agent via ENV var, local project probes, or global marker.

        LOCAL probes only fire when the global marker (`~/_GLOBAL_MARKER`)
        also exists, preventing false positives from stale project files.
        """
        if cls._is_env():
            return AgentDetectLevel.ENV
        marker = cls._global_marker_path()
        installed = marker is not None and marker.exists()
        if installed and any((project_root / p).exists() for p in cls._LOCAL_PROBES):
            return AgentDetectLevel.LOCAL
        if installed:
            return AgentDetectLevel.GLOBAL
        return None

    @classmethod
    def detect_all(cls, project_root: Path) -> List[Tuple["_AIAgent", "AgentDetectLevel"]]:
        """Return all detected AI coding agents sorted by detection level."""
        detected: List[Tuple[AgentDetectLevel, Type[_AIAgent]]] = []
        for variant_cls in AI_AGENTS.values():
            level = variant_cls._detect(project_root)
            if level is not None:
                detected.append((level, variant_cls))
        detected.sort(key=lambda t: t[0])
        return [(variant_cls(), level) for level, variant_cls in detected]

    @classmethod
    def best_detected_agents(cls, project_root: Path) -> List["_AIAgent"]:
        """Agents detected at the strongest level, empty when none is detected."""
        detected = cls.detect_all(project_root)
        return [agent for agent, level in detected if level == detected[0][1]] if detected else []

    @staticmethod
    def recorded_agent_name() -> Optional[TAIAgentName]:
        """The agent the init toolkit was installed for, from the `.toolkits` index."""
        init_entry = load_toolkits_index().get(_INIT_TOOLKIT)
        recorded = init_entry.get("agent") if isinstance(init_entry, dict) else None
        return cast(TAIAgentName, recorded) if recorded in AI_AGENTS else None

    @classmethod
    def detect_agent_name(cls, agent: Optional[str], project_root: Path) -> Optional[str]:
        """An explicit agent, the recorded one, or the single best detected one. Never raises."""
        if agent is not None:
            return agent
        if recorded := cls.recorded_agent_name():
            return recorded
        best = cls.best_detected_agents(project_root)
        return best[0].name if len(best) == 1 else None

    @staticmethod
    def resolve_installed_component(
        toolkit: str, name: str, kind: TComponentType, project_root: Path
    ) -> Optional[Path]:
        """Path of a component `dlthub ai toolkit install` wrote, or `None` when nothing matches."""
        index = load_toolkits_index()
        entry = index.get(toolkit) if toolkit else None
        # the index knows the host that installed a toolkit; otherwise try every host layout
        host = entry.get("agent") if entry else None
        variants = [AI_AGENTS[host]] if host in AI_AGENTS else list(AI_AGENTS.values())

        # an empty `toolkit` searches every installed toolkit
        toolkits = [toolkit] if toolkit else list(index)
        for variant_cls in variants:
            for toolkit_name in toolkits:
                path = variant_cls().component_path(kind, name, toolkit_name, project_root)
                if path is not None and path.is_file():
                    return path
        return None


class _ClaudeAgent(_AIAgent):
    _DIRS: ClassVar[Dict[TComponentType, str]] = {
        "skill": ".claude/skills",
        "command": ".claude/commands",
        "rule": ".claude/rules",
        "agent": f".claude/{DLTHUB_AGENTS_DIR}",
    }
    _GLOBAL_MARKER: ClassVar[str] = ".claude"
    _LOCAL_PROBES: ClassVar[Tuple[str, ...]] = (".claude", "CLAUDE.md")
    _IGNORE_FILE: ClassVar[Optional[str]] = ".claudeignore"

    @property
    def name(self) -> TAIAgentName:
        return "claude"

    @classmethod
    def _is_env(cls) -> bool:
        return is_claude_code()

    def _transform_rule(self, content: str) -> str:
        return strip_rule_frontmatter(content)

    def mcp_config_paths(self, project_root: Path) -> List[Path]:
        return [project_root / ".mcp.json"]

    def parse_mcp_servers(self, content: str) -> Dict[str, Any]:
        return parse_json_mcp(content, "mcpServers")[1]

    def merge_mcp_servers(self, existing_content: str, new_servers: Dict[str, Any]) -> str:
        return merge_json_mcp_servers(existing_content, new_servers, "mcpServers", strip_type=False)


class _CursorAgent(_AIAgent):
    _DIRS: ClassVar[Dict[TComponentType, str]] = {
        "skill": ".cursor/skills",
        "command": ".cursor/commands",
        "rule": ".cursor/rules",
        "agent": f".cursor/{DLTHUB_AGENTS_DIR}",
    }
    _GLOBAL_MARKER: ClassVar[str] = ".cursor"
    _LOCAL_PROBES: ClassVar[Tuple[str, ...]] = (".cursor", ".cursorignore", ".cursorrules")
    _RULE_EXT: ClassVar[str] = ".mdc"
    _IGNORE_FILE: ClassVar[Optional[str]] = ".cursorignore"

    @property
    def name(self) -> TAIAgentName:
        return "cursor"

    @classmethod
    def _is_env(cls) -> bool:
        return is_cursor()

    def _transform_rule(self, content: str) -> str:
        return ensure_cursor_rule_frontmatter(content)

    def install_notes(self) -> List[str]:
        return [
            "Cursor requires you to manually enable MCP servers per-project."
            " Open Cursor Settings > MCP and enable the servers installed by dlt."
        ]

    def mcp_config_paths(self, project_root: Path) -> List[Path]:
        return [project_root / ".cursor" / "mcp.json"]

    def parse_mcp_servers(self, content: str) -> Dict[str, Any]:
        return parse_json_mcp(content, "mcpServers")[1]

    def merge_mcp_servers(self, existing_content: str, new_servers: Dict[str, Any]) -> str:
        return merge_json_mcp_servers(existing_content, new_servers, "mcpServers", strip_type=True)


class _CodexAgent(_AIAgent):
    _DIRS: ClassVar[Dict[TComponentType, str]] = {
        "skill": ".agents/skills",
        "agent": f".agents/{DLTHUB_AGENTS_DIR}",
        # Codex has no rules: each rule is installed as an always-apply skill
        "rule": ".agents/skills",
    }
    _GLOBAL_MARKER: ClassVar[str] = ".codex"
    _LOCAL_PROBES: ClassVar[Tuple[str, ...]] = (".agents", "AGENTS.md")
    _IGNORE_FILE: ClassVar[Optional[str]] = ".codexignore"
    _MAX_SKILL_DESCRIPTION: ClassVar[Optional[int]] = 1024
    """Codex silently drops skills whose description exceeds 1024 characters."""

    RULES_HOOK: ClassVar[Dict[str, Any]] = {
        "matcher": "startup|resume|clear|compact",
        "hooks": [
            {
                "type": "command",
                "command": "uv run dlthub ai rules --agent codex",
                "statusMessage": "Loading dltHub rules",
            }
        ],
    }
    """SessionStart hook that gives Codex the installed rule skills. Users approve it once, so it
    must not change."""

    RULES_INTRO: ClassVar[str] = (
        "ALWAYS read and follow these dltHub skills before acting, they hold the rules of this"
        " workspace:"
    )

    MCP_DEFAULTS: ClassVar[Dict[str, Any]] = {"mcp_optional_startup_grace_ms": 5000}
    """Top-level Codex settings written with the MCP servers when missing. Codex waits 1000 ms by
    default for MCP servers before the first turn, too short for the dlt server to start."""

    @property
    def name(self) -> TAIAgentName:
        return "codex"

    @classmethod
    def _is_env(cls) -> bool:
        return is_codex()

    def hooks_config_path(self, project_root: Path) -> Path:
        return project_root / ".codex" / "hooks.json"

    def component_path(
        self,
        component_type: TComponentType,
        source_name: str,
        toolkit_name: str,
        project_root: Path,
    ) -> Optional[Path]:
        skill_dir = self.component_dir("skill", project_root)
        if component_type == "command":
            return skill_dir / source_name / COMPONENT_MARKERS["skill"]
        if component_type == "rule":
            return skill_dir / (toolkit_name + "-" + source_name) / COMPONENT_MARKERS["skill"]
        return super().component_path(component_type, source_name, toolkit_name, project_root)

    def _install_skill(
        self,
        content_or_path: Path,
        source_name: str,
        project_root: Path,
        overwrite: bool,
    ) -> List["InstallAction"]:
        actions = super()._install_skill(content_or_path, source_name, project_root, overwrite)
        skill_md = content_or_path / COMPONENT_MARKERS["skill"]
        for i, action in enumerate(actions):
            if action.content_or_path != skill_md:
                continue
            # Codex drops skills whose description exceeds the limit
            capped = cap_skill_description(
                skill_md.read_text(encoding="utf-8"), self._MAX_SKILL_DESCRIPTION
            )
            if capped is not None:
                actions[i] = action._replace(op="save", content_or_path=capped)
        return actions

    def _install_command_or_rule(
        self,
        component_type: TComponentType,
        content: str,
        source_name: str,
        toolkit_name: str,
        project_root: Path,
        overwrite: bool,
    ) -> List["InstallAction"]:
        if component_type == "command":
            wrapped = wrap_as_skill(content, source_name)
            wrapped = cap_skill_description(wrapped, self._MAX_SKILL_DESCRIPTION) or wrapped
            dest = (
                self.component_dir("skill", project_root) / source_name / COMPONENT_MARKERS["skill"]
            )
            return [
                InstallAction(
                    kind="skill",
                    source_name=source_name,
                    dest_path=dest,
                    op="save",
                    content_or_path=wrapped,
                    conflict=not overwrite and dest.exists(),
                    source_kind="command",
                )
            ]
        # rule → always-apply skill, listed by the session start hook
        skill_name = toolkit_name + "-" + source_name
        wrapped = wrap_as_skill(content, skill_name, always_apply=True)
        wrapped = cap_skill_description(wrapped, self._MAX_SKILL_DESCRIPTION) or wrapped
        skill_dest = (
            self.component_dir("skill", project_root) / skill_name / COMPONENT_MARKERS["skill"]
        )
        return [
            InstallAction(
                kind="skill",
                source_name=source_name,
                dest_path=skill_dest,
                op="save",
                content_or_path=wrapped,
                conflict=not overwrite and skill_dest.exists(),
                source_kind="rule",
            )
        ]

    def installed_rule_files(self, project_root: Path) -> List[Path]:
        # rule skills share the folder with ordinary skills
        return [
            skill_md
            for skill_md in sorted(
                self.component_dir("rule", project_root).glob("*/" + COMPONENT_MARKERS["skill"])
            )
            if is_always_apply_skill(skill_md)
        ]

    def session_context(self, project_root: Path) -> Optional[str]:
        """Links to the installed rule skills, `None` when there are none."""
        rule_skills = self.installed_rule_files(project_root)
        if not rule_skills:
            return None
        links = [
            "- `%s`: %s" % (skill_md.parent.name, skill_md.relative_to(project_root).as_posix())
            for skill_md in rule_skills
        ]
        return "\n".join([self.RULES_INTRO, *links])

    def finalize_actions(
        self, actions: List["InstallAction"], project_root: Path
    ) -> List["InstallAction"]:
        """Adds the session start hook that lists the rule skills when the toolkit has rules."""
        if not any(a.source_kind == "rule" for a in actions):
            return actions
        hooks_path = self.hooks_config_path(project_root)
        existing = hooks_path.read_text(encoding="utf-8") if hooks_path.is_file() else ""
        merged = merge_json_hooks(existing, "SessionStart", self.RULES_HOOK)
        if merged is not None:
            actions.append(
                InstallAction(
                    kind="hook",
                    source_name="SessionStart",
                    dest_path=hooks_path,
                    op="save",
                    content_or_path=merged,
                    conflict=False,
                    skip_index=True,
                )
            )
        return actions

    def install_notes(self) -> List[str]:
        return [
            "Codex runs the hook that loads dltHub rules only in a trusted project, after you"
            " approve it with /hooks (Settings > Hooks in the Codex app)."
        ]

    def mcp_config_paths(self, project_root: Path) -> List[Path]:
        return [project_root / ".codex" / "config.toml"]

    def parse_mcp_servers(self, content: str) -> Dict[str, Any]:
        return parse_toml_mcp(content, "mcp_servers")[1]

    def merge_mcp_servers(self, existing_content: str, new_servers: Dict[str, Any]) -> str:
        return merge_toml_mcp_servers(
            existing_content, new_servers, "mcp_servers", defaults=self.MCP_DEFAULTS
        )


class _CortexAgent(_AIAgent):
    """Snowflake Cortex Code: its CLI and Desktop read the project's `.cortex` folder."""

    _DIRS: ClassVar[Dict[TComponentType, str]] = {
        "skill": ".cortex/skills",
        "command": ".cortex/commands",
        "rule": ".cortex/rules",
        "agent": f".cortex/{DLTHUB_AGENTS_DIR}",
    }
    _GLOBAL_MARKER: ClassVar[str] = "cortex"
    _LOCAL_PROBES: ClassVar[Tuple[str, ...]] = (".cortex", "CORTEX.md", ".snowflake/cortex")
    _UNWANTED_DEPS: ClassVar[Tuple[str, ...]] = ("marimo", "mowidgets")
    """Workspace deps a Cortex workspace does not install."""

    RULES_HEADING: ClassVar[str] = "# dltHub rules"
    """Heading of the `CORTEX.md` section that dlt owns."""

    RULES_NOTICE: ClassVar[str] = (
        "Generated by dlthub from .cortex/rules/ on every toolkit install. Do not edit this"
        " section; edit the rule files."
    )

    @property
    def name(self) -> TAIAgentName:
        return "cortex"

    @classmethod
    def _is_env(cls) -> bool:
        return is_cortex()

    @staticmethod
    def snowflake_home() -> Optional[Path]:
        """Snowflake's config directory, `$SNOWFLAKE_HOME` or `~/.snowflake`."""
        if env_home := os.environ.get("SNOWFLAKE_HOME"):
            return Path(env_home)
        home = home_dir()
        return home / ".snowflake" if home is not None else None

    @classmethod
    def _global_marker_path(cls) -> Optional[Path]:
        sf_home = cls.snowflake_home()
        return sf_home / cls._GLOBAL_MARKER if sf_home is not None else None

    def cortex_md_path(self, project_root: Path) -> Path:
        """Path to the CORTEX.md file that holds the rules."""
        return project_root / "CORTEX.md"

    def _transform_rule(self, content: str) -> str:
        return strip_rule_frontmatter(content)

    @staticmethod
    def _section_rule_body(rule_file: Path) -> str:
        """Rule body as it goes into the `CORTEX.md` section, under the section heading."""
        body = MarkdownDocument(read_rule_body(rule_file))
        # a body that ends inside a code fence would swallow the rest of CORTEX.md
        body.close_open_blocks()
        body.demote_headings()
        return str(body)

    def render_cortex_md(self, project_root: Path) -> Optional[str]:
        """CORTEX.md with its `# dltHub rules` section built from the installed rule files,
        `None` if unchanged.
        """
        bodies = [self._section_rule_body(f) for f in self.installed_rule_files(project_root)]
        cortex_md = self.cortex_md_path(project_root)
        existing = cortex_md.read_text(encoding="utf-8") if cortex_md.is_file() else ""
        doc = MarkdownDocument(existing)
        if not bodies and doc.find_top_heading(self.RULES_HEADING) is None:
            return None
        doc.replace_section(
            self.RULES_HEADING,
            "\n\n".join([self.RULES_HEADING + "\n" + self.RULES_NOTICE, *bodies]),
        )
        merged = str(doc)
        return merged if merged != existing else None

    def post_finalize_actions(
        self, actions: List["InstallAction"], project_root: Path
    ) -> List["InstallAction"]:
        """Renders the `CORTEX.md` section from the rule files once the install wrote them."""
        return [
            InstallAction(
                kind="rule",
                source_name="CORTEX.md",
                dest_path=self.cortex_md_path(project_root),
                op="render",
                content_or_path="",
                conflict=False,
                skip_index=True,
                render=functools.partial(self.render_cortex_md, project_root),
            )
        ]

    def install_notes(self) -> List[str]:
        return [
            "Cortex Code reads skills and CORTEX.md when a session starts: start a new session.",
            (
                "Cortex Code Desktop loads project skills and MCP servers only from a trusted"
                " workspace."
            ),
        ]

    def workspace_python_dependencies(
        self, specs: List["TInstallSpec"], deps: List[str]
    ) -> Tuple[List["TInstallSpec"], List[str]]:
        """No `dlthub-client` (pulled in by the `hub` extra) and no marimo notebooks."""
        from dlt._workspace.deployment.requirements import (
            DLT_PKG_NAME,
            DLTHUB_CLIENT_PKG_NAME,
            DLTHUB_PKG_NAME,
            dlt_extra_specs,
        )

        kept_specs: List["TInstallSpec"] = []
        for spec in specs:
            if spec["name"] == DLTHUB_CLIENT_PKG_NAME:
                continue
            if spec["name"] == DLT_PKG_NAME:
                spec = cast(
                    "TInstallSpec", {**spec, "extras": [e for e in spec["extras"] if e != "hub"]}
                )
            kept_specs.append(spec)
        kept_deps = [d for d in deps if Requirement(d).name not in self._UNWANTED_DEPS]
        if not any(spec["name"] == DLTHUB_PKG_NAME for spec in kept_specs):
            kept_deps.insert(0, dlt_extra_specs("hub")[DLTHUB_PKG_NAME])
        return kept_specs, kept_deps

    def mcp_config_paths(self, project_root: Path) -> List[Path]:
        # Desktop reads the project config, the CLI only the user one
        paths = [project_root / ".cortex" / "mcp.json"]
        if (sf_home := self.snowflake_home()) is not None:
            paths.append(sf_home / "cortex" / "mcp.json")
        return paths

    def parse_mcp_servers(self, content: str) -> Dict[str, Any]:
        return parse_json_mcp(content, "mcpServers")[1]

    def merge_mcp_servers(self, existing_content: str, new_servers: Dict[str, Any]) -> str:
        # the Cortex Code CLI requires `type`; a server started by a command speaks stdio
        typed = {
            name: {"type": "stdio", **config} if "command" in config else config
            for name, config in new_servers.items()
        }
        return merge_json_mcp_servers(existing_content, typed, "mcpServers", strip_type=False)


AI_AGENTS: Dict[str, Type[_AIAgent]] = {
    "claude": _ClaudeAgent,
    "cursor": _CursorAgent,
    "codex": _CodexAgent,
    "cortex": _CortexAgent,
}
