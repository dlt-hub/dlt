from abc import ABC, abstractmethod

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.prompts.base import Prompt
from mcp.server.fastmcp.utilities.logging import get_logger

from dlt._workspace.mcp.tools.mcp_tools import PipelineMCPTools

from dlt._workspace.mcp import tools
from dlt._workspace.mcp import prompts


class DltMCP(FastMCP, ABC):
    def __init__(self, name: str, port: int = 8000, path: str = "/mcp") -> None:
        super().__init__(
            name=name,
            port=port,
            sse_path=path,
            streamable_http_path=path,
        )
        self.logger = get_logger(__name__)
        self._register_features()

    @abstractmethod
    def _register_features(self) -> None:
        pass


class WorkspaceMCP(DltMCP):
    """MCP working in Workspace context"""

    # TODO: allow to  configure in a standard way
    # @with_config(WorkspaceMCPConfiguration)
    def __init__(self, name: str = "dlt", port: int = 8000, path: str = "/mcp") -> None:
        super().__init__(
            name=name,
            port=port,
            path=path,
        )

    def _register_features(self) -> None:
        """Register MCP tools, resources, and prompts at initialization for dlt OSS."""
        for tool in tools.pipeline.__tools__:
            self.add_tool(tool)
        self.logger.debug("dlt tools registered.")

        # TODO: possibly remove docs resources. LLMs seem to use google now
        # for resource_fn in resources.docs.__resources__:
        #     self.add_resource(resource_fn())
        # self.logger.debug("dlt resources registered.")

        for prompt_fn in prompts.pipeline.__prompts__:
            self.add_prompt(Prompt.from_function(prompt_fn))
        self.logger.debug("dlt prompts registered.")


class PipelineMCP(DltMCP):
    """MCP Working in a context of particular pipeline"""

    # TODO: allow to  configure in a standard way
    # @with_config(PipelineMCPConfiguration)
    def __init__(self, pipeline_name: str, port: int = 8000, path: str = "/mcp") -> None:
        self.pipeline_name = pipeline_name
        super().__init__(
            name=f"dlt pipeline: {pipeline_name}",
            port=port,
            path=path,
        )

    def _register_features(self) -> None:
        pipeline_tools = PipelineMCPTools(self.pipeline_name)
        pipeline_tools.register_with(self)
