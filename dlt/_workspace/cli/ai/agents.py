from abc import ABC, abstractmethod
from enum import IntEnum
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Literal, NamedTuple, Optional, Tuple, Type, Union

from dlt.common.runtime.exec_info import is_claude_code, is_codex, is_cursor

from dlt._workspace.cli.ai.utils import (
    ensure_cursor_rule_frontmatter,
    home_dir,
    merge_json_mcp_servers,
    merge_toml_mcp_servers,
    parse_json_mcp,
    parse_toml_mcp,
    read_agents_md_template,
    strip_rule_frontmatter,
    wrap_as_skill,
)
from dlt._workspace.cli.formatters import merge_agents_md_skills

TComponentType = Literal["skill", "command", "rule", "ignore", "mcp"]
TInstallOp = Literal["copytree", "save"]


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


class _AIAgent(ABC):
    """AI coding agent adapter for toolkit installation, detection, and MCP configuration."""

    @property
    @abstractmethod
    def name(self) -> str: ...

    _DIRS: ClassVar[Dict[TComponentType, str]]
    """maps component types to relative directory paths under project root"""

    _GLOBAL_MARKER: ClassVar[str]
    """home-dir marker that proves the tool is installed (e.g. ".claude")"""

    _LOCAL_PROBES: ClassVar[Tuple[str, ...]]
    """project-root paths that indicate usage of this agent"""

    _RULE_EXT: ClassVar[str] = ".md"
    """file extension for rule files"""

    @property
    @abstractmethod
    def ignore_file_name(self) -> str:
        """Platform-specific ignore file name (e.g. .claudeignore)."""

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

        Handles all component types: skill (copytree), ignore (save), and
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
            dest = self.component_dir("skill", project_root) / source_name
            return [
                InstallAction(
                    kind="skill",
                    source_name=source_name,
                    dest_path=dest,
                    op="copytree",
                    content_or_path=content_or_path,
                    conflict=not overwrite and dest.exists(),
                )
            ]
        if component_type == "ignore":
            assert isinstance(content_or_path, str)
            dest = self.component_dir("ignore", project_root) / self.ignore_file_name
            return [
                InstallAction(
                    kind="ignore",
                    source_name=".claudeignore",
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
        self,
        actions: List["InstallAction"],
        project_root: Path,
        workbench_base: Optional[Path] = None,
    ) -> List["InstallAction"]:
        """Post-process the full action list. Default is identity.

        Subclasses override to coalesce shared targets (e.g. AGENTS.md).
        *workbench_base* is the root of the fetched workbench repo.
        """
        return actions

    @abstractmethod
    def mcp_config_path(self, project_root: Path) -> Path:
        """Platform-specific MCP config file path."""

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
    def _detect(cls, project_root: Path) -> Optional[AgentDetectLevel]:
        """Detect agent via ENV var, local project probes, or global marker.

        LOCAL probes only fire when the global marker (`~/_GLOBAL_MARKER`)
        also exists, preventing false positives from stale project files.
        """
        if cls._is_env():
            return AgentDetectLevel.ENV
        home = home_dir()
        installed = home is not None and (home / cls._GLOBAL_MARKER).exists()
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


class _ClaudeAgent(_AIAgent):
    _DIRS: ClassVar[Dict[TComponentType, str]] = {
        "skill": ".claude/skills",
        "command": ".claude/commands",
        "rule": ".claude/rules",
    }
    _GLOBAL_MARKER: ClassVar[str] = ".claude"
    _LOCAL_PROBES: ClassVar[Tuple[str, ...]] = (".claude", "CLAUDE.md")

    @property
    def name(self) -> str:
        return "claude"

    @classmethod
    def _is_env(cls) -> bool:
        return is_claude_code()

    @property
    def ignore_file_name(self) -> str:
        return ".claudeignore"

    def _transform_rule(self, content: str) -> str:
        return strip_rule_frontmatter(content)

    def mcp_config_path(self, project_root: Path) -> Path:
        return project_root / ".mcp.json"

    def parse_mcp_servers(self, content: str) -> Dict[str, Any]:
        return parse_json_mcp(content, "mcpServers")[1]

    def merge_mcp_servers(self, existing_content: str, new_servers: Dict[str, Any]) -> str:
        return merge_json_mcp_servers(existing_content, new_servers, "mcpServers", strip_type=False)


class _CursorAgent(_AIAgent):
    _DIRS: ClassVar[Dict[TComponentType, str]] = {
        "skill": ".cursor/skills",
        "command": ".cursor/commands",
        "rule": ".cursor/rules",
    }
    _GLOBAL_MARKER: ClassVar[str] = ".cursor"
    _LOCAL_PROBES: ClassVar[Tuple[str, ...]] = (".cursor", ".cursorignore", ".cursorrules")

    @property
    def name(self) -> str:
        return "cursor"

    @classmethod
    def _is_env(cls) -> bool:
        return is_cursor()

    _RULE_EXT: ClassVar[str] = ".mdc"

    @property
    def ignore_file_name(self) -> str:
        return ".cursorignore"

    def _transform_rule(self, content: str) -> str:
        return ensure_cursor_rule_frontmatter(content)

    def mcp_config_path(self, project_root: Path) -> Path:
        return project_root / ".cursor" / "mcp.json"

    def parse_mcp_servers(self, content: str) -> Dict[str, Any]:
        return parse_json_mcp(content, "mcpServers")[1]

    def merge_mcp_servers(self, existing_content: str, new_servers: Dict[str, Any]) -> str:
        return merge_json_mcp_servers(existing_content, new_servers, "mcpServers", strip_type=True)


class _CodexAgent(_AIAgent):
    _DIRS: ClassVar[Dict[TComponentType, str]] = {
        "skill": ".agents/skills",
    }
    _GLOBAL_MARKER: ClassVar[str] = ".codex"
    _LOCAL_PROBES: ClassVar[Tuple[str, ...]] = (".agents", "AGENTS.md")

    @property
    def name(self) -> str:
        return "codex"

    @classmethod
    def _is_env(cls) -> bool:
        return is_codex()

    @property
    def ignore_file_name(self) -> str:
        return ".codexignore"

    def agents_md_path(self, project_root: Path) -> Path:
        """Path to the AGENTS.md file for skill registration."""
        return project_root / "AGENTS.md"

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
            dest = self.component_dir("skill", project_root) / source_name / "SKILL.md"
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
        # rule → always-apply skill (AGENTS.md is handled by finalize_actions)
        skill_name = toolkit_name + "-" + source_name
        wrapped = wrap_as_skill(content, skill_name, always_apply=True)
        skill_dest = self.component_dir("skill", project_root) / skill_name / "SKILL.md"
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

    def finalize_actions(
        self,
        actions: List["InstallAction"],
        project_root: Path,
        workbench_base: Optional[Path] = None,
    ) -> List["InstallAction"]:
        """Coalesce rule-converted-to-skill actions into a single AGENTS.md merge action."""
        skill_names: List[str] = []
        for a in actions:
            if a.source_kind == "rule" and not a.conflict:
                skill_names.append(a.dest_path.parent.name)

        if not skill_names:
            return actions

        agents_md = self.agents_md_path(project_root)
        existing = agents_md.read_text(encoding="utf-8") if agents_md.is_file() else ""
        template = read_agents_md_template(workbench_base)
        merged = merge_agents_md_skills(existing, skill_names, template=template)
        if merged != existing:
            actions.append(
                InstallAction(
                    kind="rule",
                    source_name="AGENTS.md",
                    dest_path=agents_md,
                    op="save",
                    content_or_path=merged,
                    conflict=False,
                    skip_index=True,
                )
            )
        return actions

    def mcp_config_path(self, project_root: Path) -> Path:
        return project_root / ".codex" / "config.toml"

    def parse_mcp_servers(self, content: str) -> Dict[str, Any]:
        return parse_toml_mcp(content, "mcp_servers")[1]

    def merge_mcp_servers(self, existing_content: str, new_servers: Dict[str, Any]) -> str:
        return merge_toml_mcp_servers(existing_content, new_servers, "mcp_servers")


AI_AGENTS: Dict[str, Type[_AIAgent]] = {
    "claude": _ClaudeAgent,
    "cursor": _CursorAgent,
    "codex": _CodexAgent,
}
