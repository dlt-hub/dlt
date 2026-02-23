import functools
import inspect
from typing import Any, Callable, List, Optional, Set

from fastmcp import FastMCP
from fastmcp.prompts import Prompt

from dlt.common import logger
from dlt.common.configuration.plugins import manager


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
        run_kwargs: dict[str, Any] = dict(kwargs)
        if transport != "stdio":
            run_kwargs.setdefault("port", self._port)
            run_kwargs.setdefault("path", self._path)
        super().run(transport=transport, **run_kwargs)

    def _register_features(self) -> None:
        features_list = manager().hook.plug_mcp(features=self._features)
        for mcp_features in features_list:
            if mcp_features is None:
                continue
            self._register_tools(mcp_features.tools)
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

    DEFAULT_FEATURES: Set[str] = {"workspace", "pipeline"}

    def __init__(
        self,
        name: str = "dlt",
        port: int = 8000,
        path: str = "/mcp",
        extra_features: Optional[Set[str]] = None,
    ) -> None:
        features = self.DEFAULT_FEATURES | (extra_features or set())
        super().__init__(name=name, features=features, port=port, path=path)


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
