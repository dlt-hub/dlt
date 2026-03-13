import functools
import inspect
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from fastmcp import FastMCP
from fastmcp.prompts import Prompt

from dlt.common import logger
from dlt.common.configuration.plugins import manager
from dlt._workspace.cli.utils import DEFAULT_MCP_FEATURES as _DEFAULT_MCP_FEATURES


# large sentinel set used to discover all registered feature names
_ALL_FEATURE_NAMES = frozenset(
    {
        "workspace",
        "pipeline",
        "toolkit",
        "secrets",
        "context",
        # add future feature names here so they are discoverable
    }
)


def discover_features() -> Tuple[Set[str], Set[str]]:
    """Probe plugin hooks and return (all_available, extra_only) feature names.

    `all_available` is every feature that at least one hook responds to.
    `extra_only` is `all_available - WorkspaceMCP.DEFAULT_FEATURES`.
    """
    available: Set[str] = set()
    for name in _ALL_FEATURE_NAMES:
        results = manager().hook.plug_mcp(features={name})
        if any(r is not None for r in results):
            available.add(name)
    extra = available - WorkspaceMCP.DEFAULT_FEATURES
    return available, extra


def resolve_features(feature_tokens: Optional[List[str]], defaults: Set[str] = None) -> Set[str]:
    """Resolve `+name`, `-name`, `name` tokens against defaults into a feature set."""
    if defaults is None:
        defaults = WorkspaceMCP.DEFAULT_FEATURES
    if not feature_tokens:
        return set(defaults)

    result = set(defaults)
    for item in feature_tokens:
        # tokens may be comma-separated: "--features=-secrets,+context"
        # because argparse treats leading "-" as a flag, use "=" form for removals
        for token in item.split(","):
            token = token.strip()
            if not token:
                continue
            if token.startswith("-"):
                result.discard(token[1:])
            elif token.startswith("+"):
                result.add(token[1:])
            else:
                result.add(token)
    return result


def _curry_pipeline_name(fn: Callable[..., Any], pipeline_name: str) -> Callable[..., Any]:
    """Bind pipeline_name as the first argument, hiding it from the MCP schema."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return fn(pipeline_name, *args, **kwargs)

    sig = inspect.signature(fn)
    params = [p for name, p in sig.parameters.items() if name != "pipeline_name"]
    wrapper.__signature__ = sig.replace(parameters=params)  # type: ignore[attr-defined]
    wrapper.__annotations__ = {k: v for k, v in fn.__annotations__.items() if k != "pipeline_name"}
    return wrapper


class DltMCP(FastMCP):
    def __init__(self, name: str, features: Set[str], port: int = 8000, path: str = "/mcp") -> None:
        super().__init__(name=name)
        self._port = port
        self._path = path
        self._features = features
        self._register_features()

    def run(self, transport: str = "streamable-http", **kwargs: Any) -> None:
        run_kwargs: Dict[str, Any] = dict(kwargs)
        if transport != "stdio":
            run_kwargs.setdefault("port", self._port)
            run_kwargs.setdefault("path", self._path)
        super().run(transport=transport, **run_kwargs)

    def _register_features(self) -> None:
        # each plugin hook returns McpFeatures (tools, prompts, providers) or None
        features_list = manager().hook.plug_mcp(features=self._features)
        for mcp_features in features_list:
            if mcp_features is None:
                continue
            self._register_tools(mcp_features.tools)
            # prompts can be raw functions or pre-built Prompt objects
            for prompt_fn in mcp_features.prompts:
                if isinstance(prompt_fn, Prompt):
                    self.add_prompt(prompt_fn)
                else:
                    self.add_prompt(Prompt.from_function(prompt_fn))
            for provider in mcp_features.providers:
                self.add_provider(provider)
        logger.debug("dlt MCP features registered for %s.", self._features)

    def _register_tools(self, tools: List[Any]) -> None:
        for tool in tools:
            self.add_tool(tool)


class WorkspaceMCP(DltMCP):
    """MCP working in Workspace context"""

    DEFAULT_FEATURES: Set[str] = set(_DEFAULT_MCP_FEATURES)

    def __init__(
        self,
        name: str = "dlt",
        port: int = 8000,
        path: str = "/mcp",
        features: Optional[Set[str]] = None,
        extra_features: Optional[Set[str]] = None,
    ) -> None:
        if features is not None:
            resolved = features
        else:
            resolved = self.DEFAULT_FEATURES | (extra_features or set())
        super().__init__(name=name, features=resolved, port=port, path=path)


class PipelineMCP(DltMCP):
    """MCP working in a context of particular pipeline"""

    def __init__(self, pipeline_name: str, port: int = 8000, path: str = "/mcp") -> None:
        self.pipeline_name = pipeline_name
        super().__init__(
            name=f"dlt pipeline: {pipeline_name}",
            features={"pipeline"},
            port=port,
            path=path,
        )

    def _register_tools(self, tools: List[Any]) -> None:
        for tool_fn in tools:
            self.add_tool(_curry_pipeline_name(tool_fn, self.pipeline_name))
