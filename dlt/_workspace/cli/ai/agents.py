from abc import ABC, abstractmethod
from enum import IntEnum
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Literal, NamedTuple, Optional, Tuple, Type, Union

from dlt._workspace.cli.ai.utils import (
    ensure_cursor_rule_frontmatter,
    home_dir,
    merge_json_mcp_servers,
    merge_toml_mcp_servers,
    parse_json_mcp,
    parse_toml_mcp,
    strip_rule_frontmatter,
    wrap_as_skill,
)

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


class _AIAgent(ABC):
    """Maps toolkit component types to agent-specific paths and formats."""

    @property
    @abstractmethod
    def name(self) -> str: ...

    _DIRS: ClassVar[Dict[TComponentType, str]]
    """maps component types to relative directory paths under project root"""

    _GLOBAL_MARKER: ClassVar[str]
    """home-dir marker that proves the tool is installed (e.g. ".claude")"""

    _LOCAL_PROBES: ClassVar[Tuple[str, ...]]
    """project-root paths that indicate usage of this agent"""

    @property
    @abstractmethod
    def ignore_file_name(self) -> str:
        """Platform-specific ignore file name (e.g. .claudeignore)."""

    def component_dir(self, component_type: TComponentType, project_root: Path) -> Path:
        """Root directory for the given component type.

        Raises NotImplementedError for types the agent does not support
        natively (e.g. codex has no command/rule dirs -- transform converts
        them to skills first).
        """
        if component_type == "ignore":
            return project_root
        if component_type not in self._DIRS:
            raise NotImplementedError(
                "%s not supported by %s (transform converts to skill)" % (component_type, self.name)
            )
        return project_root / self._DIRS[component_type]

    def transform(
        self,
        component_type: TComponentType,
        content: str,
        source_name: str,
        toolkit_name: str,
    ) -> Tuple[TComponentType, str, str]:
        """Transform content for this agent.

        Args:
            component_type: "skill", "command", or "rule"
            content: raw file content (markdown with possible frontmatter)
            source_name: original name (stem, e.g. "find-source", "bootstrap")
            toolkit_name: toolkit identifier (e.g. "rest-api-pipeline")

        Returns:
            (output_type, output_content, output_filename)
            output_type may differ from input (e.g. codex: rule -> skill)
            output_filename is the destination filename or dir name
        """
        if component_type == "skill":
            return ("skill", content, source_name)
        if component_type == "ignore":
            return ("ignore", content, self.ignore_file_name)
        return self._transform_command_or_rule(component_type, content, source_name, toolkit_name)

    @abstractmethod
    def _transform_command_or_rule(
        self,
        component_type: TComponentType,
        content: str,
        source_name: str,
        toolkit_name: str,
    ) -> Tuple[TComponentType, str, str]:
        """Agent-specific transform for commands and rules."""

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
        from dlt.common.runtime.exec_info import is_claude_code

        return is_claude_code()

    @property
    def ignore_file_name(self) -> str:
        return ".claudeignore"

    def _transform_command_or_rule(
        self,
        component_type: TComponentType,
        content: str,
        source_name: str,
        toolkit_name: str,
    ) -> Tuple[TComponentType, str, str]:
        if component_type == "command":
            return ("command", content, source_name + ".md")
        transformed = strip_rule_frontmatter(content)
        return ("rule", transformed, toolkit_name + "-" + source_name + ".md")

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
        from dlt.common.runtime.exec_info import is_cursor

        return is_cursor()

    @property
    def ignore_file_name(self) -> str:
        return ".cursorignore"

    def _transform_command_or_rule(
        self,
        component_type: TComponentType,
        content: str,
        source_name: str,
        toolkit_name: str,
    ) -> Tuple[TComponentType, str, str]:
        if component_type == "command":
            return ("command", content, source_name + ".md")
        transformed = ensure_cursor_rule_frontmatter(content)
        return ("rule", transformed, toolkit_name + "-" + source_name + ".mdc")

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
        from dlt.common.runtime.exec_info import is_codex

        return is_codex()

    @property
    def ignore_file_name(self) -> str:
        return ".codexignore"

    def _transform_command_or_rule(
        self,
        component_type: TComponentType,
        content: str,
        source_name: str,
        toolkit_name: str,
    ) -> Tuple[TComponentType, str, str]:
        if component_type == "command":
            wrapped = wrap_as_skill(content, source_name)
            return ("skill", wrapped, source_name)
        name = toolkit_name + "-" + source_name
        wrapped = wrap_as_skill(content, name, always_apply=True)
        return ("skill", wrapped, name)

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
