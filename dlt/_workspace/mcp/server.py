from abc import ABC, abstractmethod
import os
import pathlib
from typing import List, Optional

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.prompts.base import Prompt
from mcp.server.fastmcp.utilities.logging import get_logger

from dlt._workspace.mcp.tools.mcp_tools import PipelineMCPTools
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase

from dlt._workspace.mcp import tools
from dlt._workspace.mcp import prompts
from dlt.pipeline.pipeline import Pipeline


class DltMCP(FastMCP, ABC):
    def __init__(
        self, name: str, dependencies: List[str], port: int = 8000, sse_path: str = "/"
    ) -> None:
        super().__init__(
            name=name,
            dependencies=dependencies,
            # log_level="WARNING",  # do not send INFO logs because some clients HANG
            port=port,
            sse_path=sse_path,
        )
        self._cwd_at_init: pathlib.Path = pathlib.Path.cwd()
        self._run_context: Optional[RunContextBase] = None
        self.logger = get_logger(__name__)

        self._register_features()

    # override `run` to allow to pass `transport` in configuration
    # def run(self, transport = "stdio", mount_path = None):
    #     return super().run(transport, mount_path)

    @abstractmethod
    def _register_features(self) -> None:
        pass


class WorkspaceMCP(DltMCP):
    """MCP working in Workspace context"""

    # TODO: allow to  configure in a standard way
    # @with_config(WorkspaceMCPConfiguration)
    def __init__(self, name: str = "dlt", port: int = 8000, sse_path: str = "/") -> None:
        super().__init__(
            name=name,
            dependencies=["dlt[workspace]"],
            port=port,
            sse_path=sse_path,
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
    def __init__(self, pipeline: Pipeline, port: int = 8000, sse_path: str = "/") -> None:
        self.pipeline = pipeline
        super().__init__(
            name=f"dlt pipeline: {pipeline.pipeline_name}",
            dependencies=["dlt[workspace]"],
            port=port,
            sse_path=sse_path,
        )

    def _register_features(self) -> None:
        pipeline_tools = PipelineMCPTools(self.pipeline)
        pipeline_tools.register_with(self)
